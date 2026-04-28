# -*- coding: utf-8 -*-
"""
DataSourceFactory — 复权 / 请求去重 / 单源 fallback / 竞赛

定位:
  KlineService(缓存层) → DataSourceFactory(本层) → Coordinator(协助层)

职责:
  1. 复权:   统一调用 adjust_kline 处理前/后复权
  2. 请求去重: InflightDedup 防止同一 symbol 并发重复请求
  3. 单源 fallback: sequential_fallback 按优先级逐源尝试
  4. 竞赛:   race 多源同时取，第一个有效结果返回
  5. 批量协调: 通过 Coordinator.coordinate_kline 按源分组并发
  6. 熔断:   CircuitBreaker 保护故障源

不负责:
  - 缓存读写（由 KlineService 处理）
  - 直接与 Provider 通信（通过 Coordinator）

调用链:
  单只K线:  DataSourceFactory.fetch_kline → dedup → sequential_fallback → adjust
  批量K线:  DataSourceFactory.fetch_kline_batch → Coordinator → adjust
  单只行情: DataSourceFactory.fetch_ticker → dedup → sequential_fallback
  批量行情: DataSourceFactory.fetch_ticker_batch → fetch_quotes_batch → race fallback
"""

from __future__ import annotations

import concurrent.futures
import threading
from typing import Any, Callable, Dict, List, Optional, Tuple, TypeVar

from app.data_sources.adjustment import adjust_kline
from app.data_sources.circuit_breaker import CircuitBreaker, get_realtime_circuit_breaker
from app.data_sources.coordinator import get_coordinator
from app.data_sources.normalizer import detect_market, to_canonical, normalize_hk_code
from app.data_sources.provider import get_providers
from app.utils.logger import get_logger

logger = get_logger(__name__)

T = TypeVar("T")


# ================================================================
# 请求去重 — 同一 symbol 正在取时，等结果不重复发
# ================================================================

class InflightDedup:
    """
    请求去重器。

    如果 symbol A 正在被某个线程取数据，其他线程对 A 的请求
    直接等结果，不重复发 API 调用。
    """

    def __init__(self, max_workers: int = 4):
        self._lock = threading.Lock()
        self._inflight: Dict[str, concurrent.futures.Future] = {}
        self._executor = concurrent.futures.ThreadPoolExecutor(
            max_workers=max_workers, thread_name_prefix="dedup",
        )

    def get_or_submit(self, key: str, fn: Callable[[], T]) -> T:
        """
        获取已有结果或提交新请求。

        如果 key 已有 inflight 请求，等结果。
        否则提交新请求到内部线程池，其他线程可以等这个结果。
        """
        with self._lock:
            if key in self._inflight:
                future = self._inflight[key]
            else:
                future = self._executor.submit(fn)
                self._inflight[key] = future

        try:
            return future.result(timeout=30)
        finally:
            with self._lock:
                if key in self._inflight and self._inflight[key] is future:
                    del self._inflight[key]


# ================================================================
# 单源 Fallback — 逐源尝试，成功就停
# ================================================================

def sequential_fallback(
    symbol: str,
    providers: List[Tuple[str, Callable[[], Optional[T]]]],
    cb: CircuitBreaker,
    validate: Callable[[T], bool] = lambda x: x is not None,
) -> Tuple[Optional[T], Optional[str]]:
    """
    顺序 fallback — 一个一个试，成功就停。

    适合 K线（每只1次HTTP，race浪费API）。
    按 priority 顺序尝试，第一个成功的立即返回。

    Args:
        symbol:     股票代码
        providers:  [(name, fetcher), ...]
        cb:         熔断器
        validate:   结果校验

    Returns:
        (result, source_name) 或 (None, None)
    """
    for name, fetcher in providers:
        if not cb.is_available(name):
            continue
        try:
            result = fetcher()
            if validate(result):
                cb.record_success(name)
                return result, name
            cb.record_failure(name, "empty/invalid")
        except Exception as e:
            cb.record_failure(name, str(e))

    return None, None


# ================================================================
# 竞赛 Race — 多源同时取，第一个有效结果返回
# ================================================================

def race(
    providers: List[Tuple[str, Callable[[], Optional[T]]]],
    cb: CircuitBreaker,
    timeout: float = 8.0,
    validate: Callable[[T], bool] = lambda x: x is not None,
) -> Tuple[Optional[T], Optional[str]]:
    """
    并发竞赛 — 多源同时取，第一个有效结果返回。

    适合行情（有批量接口，race代价低）。
    不适合K线（逐只取，race浪费API）。
    """
    available = [(n, f) for n, f in providers if cb.is_available(n)]
    if not available:
        return None, None

    if len(available) == 1:
        name, fn = available[0]
        try:
            result = fn()
            if validate(result):
                cb.record_success(name)
                return result, name
            cb.record_failure(name, "empty/invalid")
        except Exception as e:
            cb.record_failure(name, str(e))
        return None, None

    result_holder: Dict[str, Any] = {"result": None, "source": None}
    done_event = threading.Event()
    lock = threading.Lock()

    def _try(source_name: str, fetcher: Callable) -> None:
        try:
            data = fetcher()
            if done_event.is_set():
                return
            if validate(data):
                with lock:
                    if result_holder["result"] is None:
                        result_holder["result"] = data
                        result_holder["source"] = source_name
                        done_event.set()
                cb.record_success(source_name)
            else:
                cb.record_failure(source_name, "empty/invalid")
        except Exception as e:
            if not done_event.is_set():
                cb.record_failure(source_name, str(e))

    executor = concurrent.futures.ThreadPoolExecutor(max_workers=len(available))
    try:
        futures = {executor.submit(_try, n, f): n for n, f in available}
        done_event.wait(timeout=timeout + 1)
    finally:
        executor.shutdown(wait=False)

    return result_holder["result"], result_holder["source"]


# ================================================================
# 市场类型常量
# ================================================================

MARKET_CN_STOCK = "CNStock"
MARKET_HK_STOCK = "HKStock"
MARKET_US_STOCK = "USStock"
MARKET_CRYPTO   = "Crypto"
MARKET_FOREX    = "Forex"
MARKET_FUTURES  = "Futures"

# 市场别名 → 标准市场名
_MARKET_ALIASES = {
    "CNStock":  MARKET_CN_STOCK,
    "HKStock":  MARKET_HK_STOCK,
    "USStock":  MARKET_US_STOCK,
    "Crypto":   MARKET_CRYPTO,
    "Forex":    MARKET_FOREX,
    "Futures":  MARKET_FUTURES,
    "CN":       MARKET_CN_STOCK,
    "HK":       MARKET_HK_STOCK,
    "US":       MARKET_US_STOCK,
    "A":        MARKET_CN_STOCK,
    "A股":      MARKET_CN_STOCK,
    "港股":     MARKET_HK_STOCK,
    "美股":     MARKET_US_STOCK,
    "加密":     MARKET_CRYPTO,
    "外汇":     MARKET_FOREX,
    "期货":     MARKET_FUTURES,
}


def _resolve_market(market: str, symbol: str) -> str:
    """
    解析市场类型。

    优先用传入的 market 参数；如果为空，根据 symbol 自动推断。
    """
    if market:
        return _MARKET_ALIASES.get(market, market)

    s = (symbol or "").strip().upper()

    # Crypto: BTC/USDT, ETH/USDT 格式
    if "/" in s:
        return MARKET_CRYPTO

    # A股 / 港股: detect_market 从代码格式推断
    exchange, _ = detect_market(symbol)
    if exchange in ("SH", "SZ", "BJ"):
        return MARKET_CN_STOCK
    if exchange == "HK":
        return MARKET_HK_STOCK

    return ""


def _parse_symbols(symbol: str) -> List[str]:
    """拆分逗号分隔的股票代码，返回去重后的非空列表"""
    return [s.strip() for s in symbol.split(",") if s.strip()]


def _normalize_symbols(symbols: List[str], market: str) -> List[str]:
    """
    根据市场类型给裸代码补前缀。

    A股: "600519" → "SH600519", "000001" → "SZ000001"
    港股: "700" → "HK00700", "00700" → "HK00700"
    已有前缀的不处理: "SH600519" → "SH600519"
    """
    result = []
    for sym in symbols:
        if market == "HKStock":
            result.append(normalize_hk_code(sym))
        else:
            canon = to_canonical(sym)
            result.append(canon if canon else sym)
    return result


# ================================================================
# DataSourceFactory — 复权 / 去重 / fallback / 竞赛
# ================================================================

class DataSourceFactory:
    """
    数据源工厂层 — 负责复权、请求去重、单源 fallback、竞赛。

    位于 KlineService(缓存层) 和 Coordinator(协助层) 之间。
    KlineService 只与本层通讯，本层只与 Coordinator 单线联系。
    """

    def __init__(self):
        self._cb = get_realtime_circuit_breaker()
        self._dedup = InflightDedup()
        self._coordinator = get_coordinator()

    # ═══════════════════════════════════════════════════════════════════
    #  K线 — 单只
    # ═══════════════════════════════════════════════════════════════════

    def fetch_kline(
        self,
        symbol: str,
        timeframe: str,
        limit: int,
        market: str,
        adj: str = "qfq",
    ) -> Optional[List[Dict[str, Any]]]:
        """
        获取单只K线 — 去重 + fallback + 复权。

        流程:
          1. InflightDedup 去重（同一 symbol 不重复发请求）
          2. sequential_fallback 按优先级逐源尝试
          3. adjust_kline 统一复权
          4. 返回复权后数据

        Returns:
            复权后的 K线列表，失败返回 None
        """
        raw = self.fetch_kline_raw(symbol, timeframe, limit, market)
        if raw:
            return adjust_kline(symbol, raw, adj)
        return None

    def fetch_kline_raw(
        self, symbol: str, timeframe: str, limit: int, market: str,
    ) -> Optional[List]:
        """
        获取单只K线原始数据 — 去重 + fallback，不复权。

        供 KlineService 缓存层调用：
          - 首次获取: fetch_kline_raw → 缓存原始 → adjust_kline → 返回
          - 缓存命中: 读原始 → adjust_kline → 返回

        Returns:
            原始(未复权) K线数据，失败返回 None
        """
        return self._dedup.get_or_submit(
            f"kline:{symbol}:{timeframe}:{limit}",
            lambda: self._do_sequential_fallback(symbol, timeframe, limit, market),
        )

    def _do_sequential_fallback(
        self, symbol: str, timeframe: str, limit: int, market: str,
    ) -> Optional[List]:
        """内部: sequential_fallback 取原始K线数据"""
        providers = [
            (p.name, lambda p=p: p.fetch_kline(symbol, timeframe, limit))
            for p in get_providers("kline", timeframe=timeframe, market=market or None)
        ]
        result, src = sequential_fallback(symbol, providers, self._cb)
        if result:
            logger.info(f"[K线] {symbol} tf={timeframe} 来源={src} bars={len(result)}")
        else:
            names = [n for n, _ in providers]
            logger.warning(f"[K线] {symbol} tf={timeframe} 全部源失败: {names}")
        return result

    # ═══════════════════════════════════════════════════════════════════
    #  K线 — 批量
    # ═══════════════════════════════════════════════════════════════════

    def fetch_kline_batch(
        self,
        symbols: List[str],
        timeframe: str,
        limit: int,
        market: str,
        adj: str = "qfq",
        on_raw_data: Optional[Callable[[str, List[Dict[str, Any]]], None]] = None,
    ) -> Dict[str, List[Dict[str, Any]]]:
        """
        批量K线 — 通过 Coordinator 协调 + 统一复权。

        流程:
          1. Coordinator.coordinate_kline 按源分组并发获取
          2. 对每个 symbol: 回调 on_raw_data(sym, raw_bars) 写缓存
          3. adjust_kline 统一复权
          4. 返回 {symbol: 复权后K线}

        Args:
            on_raw_data: 可选回调，每个 symbol 获取到原始数据时调用，
                         供缓存层写入原始数据。签名: (symbol, raw_bars) → None

        Returns:
            {symbol: kline_bars} — 仅包含成功获取的 symbol
        """
        providers = get_providers("kline", timeframe=timeframe, market=market or None)

        fetched, failed = self._coordinator.coordinate_kline(
            symbols=symbols,
            timeframe=timeframe,
            limit=limit,
            providers=providers,
            cb=self._cb,
            market=market,
        )

        if failed:
            logger.warning(
                f"[批量K线] {len(failed)} 只所有源均失败: "
                f"{failed[:5]}{'...' if len(failed) > 5 else ''}"
            )

        # 回调写缓存 + 统一复权
        result = {}
        for sym, bars in fetched.items():
            if bars:
                # 回调缓存层写入原始数据（复权前）
                if on_raw_data is not None:
                    try:
                        on_raw_data(sym, bars)
                    except Exception as e:
                        logger.warning(f"[批量K线] on_raw_data 回调失败 {sym}: {e}")
                result[sym] = adjust_kline(sym, bars, adj)

        logger.info(f"[批量K线] DataSourceFactory: {len(result)}/{len(symbols)} 成功")
        return result

    # ═══════════════════════════════════════════════════════════════════
    #  行情 — 单只
    # ═══════════════════════════════════════════════════════════════════

    def fetch_ticker(
        self,
        symbol: str,
        market: str,
    ) -> Optional[Dict[str, Any]]:
        """
        获取单只行情 — 去重 + fallback。

        流程:
          1. InflightDedup 去重
          2. sequential_fallback 按优先级逐源尝试
          3. 返回行情数据
        """
        result = self._dedup.get_or_submit(
            f"ticker:{symbol}",
            lambda: self._fetch_ticker_raw(symbol, market),
        )
        return result

    def _fetch_ticker_raw(
        self, symbol: str, market: str,
    ) -> Optional[Dict[str, Any]]:
        """内部: sequential_fallback 取行情"""
        providers = [
            (p.name, lambda p=p: p.fetch_quote(symbol))
            for p in get_providers("quote", market=market or None)
        ]
        result, src = sequential_fallback(symbol, providers, self._cb)
        if result:
            logger.info(f"[行情] {symbol} 来源={src}")
        else:
            names = [n for n, _ in providers]
            logger.warning(f"[行情] {symbol} 全部源失败: {names}")
        return result

    # ═══════════════════════════════════════════════════════════════════
    #  行情 — 批量
    # ═══════════════════════════════════════════════════════════════════

    def fetch_ticker_batch(
        self,
        symbols: List[str],
        market: str,
    ) -> Dict[str, Dict[str, Any]]:
        """
        批量行情 — fetch_quotes_batch 一次HTTP取多只 + 逐只 fallback。

        流程:
          1. 按 Provider 优先级调用 fetch_quotes_batch（一次HTTP取多只）
          2. 未取到的 symbol fallback 到逐只 sequential_fallback
          3. 返回 {symbol: 行情数据}
        """
        providers = get_providers("quote", market=market or None)

        # 尝试批量接口
        for p in providers:
            if not self._cb.is_available(p.name):
                continue
            try:
                batch = p.fetch_quotes_batch(symbols)
                if batch:
                    self._cb.record_success(p.name)
                    logger.info(
                        f"[批量行情] {p.name} 一次HTTP取到 {len(batch)}/{len(symbols)} 只"
                    )
                    return batch
                self._cb.record_failure(p.name, "empty")
            except Exception as e:
                self._cb.record_failure(p.name, str(e))
                logger.warning(f"[批量行情] {p.name} 批量获取失败: {e}")

        # 所有批量源都失败 → fallback 到逐只
        logger.info("[批量行情] 所有批量源失败，fallback 到逐只模式")
        result: Dict[str, Dict[str, Any]] = {}
        for sym in symbols:
            try:
                data = self.fetch_ticker(sym, market)
                if data and data.get("last", 0) > 0:
                    result[sym] = data
            except Exception:
                pass
        return result

    # ═══════════════════════════════════════════════════════════════════
    #  公共工具 — 供 KlineService 调用
    # ═══════════════════════════════════════════════════════════════════

    @staticmethod
    def resolve_market(market: str, symbol: str) -> str:
        """解析市场类型（公开接口）"""
        return _resolve_market(market, symbol)

    @staticmethod
    def parse_symbols(symbol: str) -> List[str]:
        """拆分逗号分隔的股票代码"""
        return _parse_symbols(symbol)

    @staticmethod
    def normalize_symbols(symbols: List[str], market: str) -> List[str]:
        """根据市场类型给裸代码补前缀"""
        return _normalize_symbols(symbols, market)

    def source_stats(self) -> Dict[str, Any]:
        """各数据源吞吐统计"""
        from app.data_sources.source_config import get_all_enabled_sources
        return {
            cfg.name: {
                "qps": round(cfg.throughput, 2),
                "success_rate": round(cfg.success_rate, 3),
                "avg_latency": round(cfg.avg_latency, 3),
                "effective_weight": round(cfg.effective_weight(), 2),
                "max_workers": cfg.max_workers,
                "markets": list(cfg.markets),
            }
            for cfg in get_all_enabled_sources()
        }


# ================================================================
# SourceAdapter — 包装 Provider，暴露旧 BaseDataSource 接口
# ================================================================

class SourceAdapter:
    """
    适配器：将新 Provider Protocol 包装为旧 BaseDataSource 接口。

    旧代码: ds = DataSourceFactory.get_source("CNStock")
            ds.get_kline(symbol, tf, limit, before_time, after_time)
            ds.get_ticker(symbol)

    新代码: 内部调用 fetch_kline_raw / fetch_ticker + adjust
    """

    def __init__(self, market: str):
        self._market = market
        self._factory = get_factory()

    def get_kline(
        self,
        symbol: str,
        timeframe: str,
        limit: int,
        before_time: Optional[int] = None,
        after_time: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        """兼容旧 BaseDataSource.get_kline 接口"""
        raw = self._factory.fetch_kline_raw(
            symbol, timeframe, limit, self._market,
        )
        if not raw:
            return []
        adjusted = adjust_kline(symbol, raw, "qfq")
        # after_time 过滤（回测左边界）
        if after_time and adjusted:
            adjusted = [b for b in adjusted if b.get("time", 0) >= after_time]
        # before_time 过滤
        if before_time and adjusted:
            adjusted = [b for b in adjusted if b.get("time", 0) < before_time]
        adjusted.sort(key=lambda x: x.get("time", 0))
        return adjusted

    def get_ticker(self, symbol: str) -> Dict[str, Any]:
        """兼容旧 BaseDataSource.get_ticker 接口"""
        result = self._factory.fetch_ticker(symbol, self._market)
        return result or {"last": 0, "symbol": symbol}


# ================================================================
# DataSourceFactory classmethod 兼容层
# ================================================================
#
# 旧调用方式（QuantDinger 现有代码）:
#   DataSourceFactory.get_kline(market, symbol, tf, limit)
#   DataSourceFactory.get_kline_batch(market, symbols, tf, limit)
#   DataSourceFactory.get_ticker(market, symbol)
#   DataSourceFactory.get_source(market)  → SourceAdapter
#   DataSourceFactory.normalize_market(market)
#
# 新调用方式（kline_service 内部）:
#   get_factory().fetch_kline_raw(symbol, tf, limit, market)
#   get_factory().fetch_kline_batch(symbols, tf, limit, market)
#   get_factory().fetch_ticker(symbol, market)

# 以下 classmethod 直接挂到 DataSourceFactory 类上，
# 由全局 _factory 实例代理，QuantDinger 现有代码无需修改。

def _cm_get_kline(
    cls, market: str, symbol: str, timeframe: str, limit: int,
    before_time: Optional[int] = None, after_time: Optional[int] = None,
) -> List[Dict[str, Any]]:
    """兼容旧 DataSourceFactory.get_kline() — classmethod 入口"""
    source = SourceAdapter(market)
    return source.get_kline(symbol, timeframe, limit, before_time, after_time)


def _cm_get_kline_batch(
    cls, market: str, symbols: List[str], timeframe: str, limit: int,
    cached_symbols: Optional[set] = None,
) -> Dict[str, List[Dict[str, Any]]]:
    """兼容旧 DataSourceFactory.get_kline_batch() — classmethod 入口"""
    resolved = _resolve_market(market, symbols[0] if symbols else "")
    return _factory.fetch_kline_batch(symbols, timeframe, limit, resolved)


def _cm_get_ticker(cls, market: str, symbol: str) -> Dict[str, Any]:
    """兼容旧 DataSourceFactory.get_ticker() — classmethod 入口"""
    resolved = _resolve_market(market, symbol)
    result = _factory.fetch_ticker(symbol, resolved)
    return result or {"last": 0, "symbol": symbol}


def _cm_get_source(cls, market: str) -> SourceAdapter:
    """
    兼容旧 DataSourceFactory.get_source() — 返回 SourceAdapter。

    旧代码:
        ds = DataSourceFactory.get_source("CNStock")
        klines = ds.get_kline(symbol, tf, limit)
        ticker = ds.get_ticker(symbol)
    """
    return SourceAdapter(market)


def _cm_get_data_source(cls, name: str) -> SourceAdapter:
    """兼容旧 DataSourceFactory.get_data_source() — 向后兼容别名"""
    key = (name or "").strip().lower()
    market_map = {
        "crypto": "Crypto", "binance": "Crypto", "okx": "Crypto",
        "bybit": "Crypto", "bitget": "Crypto", "kucoin": "Crypto",
        "gate": "Crypto", "mexc": "Crypto", "kraken": "Crypto", "coinbase": "Crypto",
        "futures": "Futures", "forex": "Forex", "fx": "Forex",
        "cnstock": "CNStock", "hkstock": "HKStock", "usstock": "USStock",
    }
    market = market_map.get(key, "Crypto")
    return SourceAdapter(market)


# 挂载 classmethod 到 DataSourceFactory
DataSourceFactory.get_kline = classmethod(_cm_get_kline)           # type: ignore
DataSourceFactory.get_kline_batch = classmethod(_cm_get_kline_batch)  # type: ignore
DataSourceFactory.get_ticker = classmethod(_cm_get_ticker)         # type: ignore
DataSourceFactory.get_source = classmethod(_cm_get_source)         # type: ignore
DataSourceFactory.get_data_source = classmethod(_cm_get_data_source)  # type: ignore
DataSourceFactory.normalize_market = staticmethod(_resolve_market)  # type: ignore


# ================================================================
# 全局实例
# ================================================================

_factory = DataSourceFactory()


def get_factory() -> DataSourceFactory:
    """获取全局 DataSourceFactory 实例"""
    return _factory
