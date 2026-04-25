# -*- coding: utf-8 -*-
"""
K线数据服务 — 新架构

上层入口，统一调度:
  用户请求 → 分类(市场/数据类型) → 查缓存 → 命中直接返回
                                 → 未命中 → 调度层取数据
                                          → 写缓存 → 复权 → 返回

两层存储:
  热数据（行情/分钟线） → 内存 dict，丢了30s重取
  温数据（K线/股票信息） → feather 文件，进程重启还在

调用链:
  KlineService.get_kline(symbol, tf, limit) → 查缓存 → sequential_fallback → adjust_kline → 返回
  KlineService.get_ticker(symbol)           → 查缓存 → race               → 返回
  KlineService.get_kline_batch(symbols)     → 查缓存 → dynamic_queue      → 返回
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from app.data_sources.cache import get_cache, make_key, get_ttl
from app.data_sources.dispatcher import sequential_fallback, dynamic_queue, InflightDedup
from app.data_sources.provider import get_providers
from app.data_sources.circuit_breaker import get_realtime_circuit_breaker
from app.data_sources.normalizer import detect_market
from app.data_sources.adjustment import adjust_kline
from app.utils.logger import get_logger

logger = get_logger(__name__)

# ================================================================
# 市场类型常量
# ================================================================
# 与旧项目 DataSourceFactory 保持一致，6 种市场类型。
# 当前已实现 Provider: CNStock, HKStock
# 待实现 Provider:     USStock, Crypto, Forex, Futures

MARKET_CN_STOCK = "CNStock"   # A股 — kline / quote / batch_quote / 复权
MARKET_HK_STOCK = "HKStock"   # 港股 — kline / quote
MARKET_US_STOCK = "USStock"   # 美股 — kline / quote (待实现, 旧项目用 yfinance/twelvedata)
MARKET_CRYPTO   = "Crypto"    # 加密货币 — kline / quote (待实现, 旧项目用 ccxt)
MARKET_FOREX    = "Forex"     # 外汇 — kline / quote (待实现, 旧项目用 twelvedata/tiingo)
MARKET_FUTURES  = "Futures"   # 期货 — kline / quote (待实现, 旧项目用 ccxt)

# 市场别名 → 标准市场名
_MARKET_ALIASES = {
    "CNStock":  MARKET_CN_STOCK,
    "HKStock":  MARKET_HK_STOCK,
    "USStock":  MARKET_US_STOCK,
    "Crypto":   MARKET_CRYPTO,
    "Forex":    MARKET_FOREX,
    "Futures":  MARKET_FUTURES,
    # 简写
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

    # 自动推断: detect_market 返回 (exchange, digits)
    s = (symbol or "").strip().upper()

    # Crypto: BTC/USDT, ETH/USDT 格式
    if "/" in s:
        return MARKET_CRYPTO

    # Forex: EUR/USD, USD/JPY 格式（同 Crypto 用 / 分隔）
    # 需要调用方传 market 区分，或后续加 forex symbol 列表判断

    # A股 / 港股: detect_market 从代码格式推断
    exchange, _ = detect_market(symbol)
    if exchange in ("SH", "SZ", "BJ"):
        return MARKET_CN_STOCK
    if exchange == "HK":
        return MARKET_HK_STOCK

    # 美股: 纯字母代码（AAPL, TSLA），需调用方传 market
    # 期货: 合约代码（如 BTC2306），需调用方传 market

    return ""


class KlineService:
    """K线数据服务（新架构：Provider自注册 + 两层缓存 + 统一复权）"""

    def __init__(self):
        self._cache = get_cache()
        self._cb = get_realtime_circuit_breaker()
        self._dedup = InflightDedup()

    # ═══════════════════════════════════════════════════════════════════
    #  对外入口
    # ═══════════════════════════════════════════════════════════════════

    def get_kline(
        self,
        market: str,
        symbol: str,
        timeframe: str,
        limit: int = 1000,
        before_time: Optional[int] = None,
        adj: str = "qfq",
    ) -> List[Dict[str, Any]]:
        """
        获取K线 — 缓存原始数据，上层统一复权。

        流程:
          1. 查缓存(原始数据) → 命中 → 复权 → 返回
          2. 未命中 → Provider 取原始数据 → 写缓存 → 复权 → 返回

        Args:
            market:    市场类型（兼容旧接口，新架构不使用）
            symbol:    股票代码（任意格式，normalizer 自动识别）
            timeframe: 周期 ('1m','5m','15m','30m','1H','1D','1W','1M')
            limit:     数据条数
            before_time: 分页用，获取此时间之前的数据（可选）
            adj:       复权方式 ('qfq'/'hfq'/''不复权)
        """
        resolved_market = _resolve_market(market, symbol)

        # before_time 模式不缓存（历史翻页）
        if before_time:
            raw = self._fetch_kline(symbol, timeframe, limit, resolved_market)
            return adjust_kline(symbol, raw or [], adj)

        key = make_key("kline", symbol, timeframe, limit)
        data_type = f"kline:{timeframe}"

        # 1. 查缓存（原始数据）
        cached = self._cache.get(key, data_type)
        if cached is not None:
            return adjust_kline(symbol, cached, adj)

        # 2. 去重 + 取原始数据
        raw = self._dedup.get_or_submit(
            key, lambda: self._fetch_kline(symbol, timeframe, limit, resolved_market)
        )

        # 3. 写缓存（原始数据）
        if raw:
            self._cache.set(key, raw, get_ttl("kline", timeframe), data_type)

        # 4. 复权后返回
        return adjust_kline(symbol, raw or [], adj)

    def get_kline_batch(
        self,
        market: str,
        symbols: List[str],
        timeframe: str,
        limit: int,
        cached_symbols: Optional[set] = None,
    ) -> Dict[str, List[Dict[str, Any]]]:
        """
        批量K线 — 核心优化路径:

        1. 查缓存（磁盘 feather）→ 命中直接用（0次API）
        2. 未命中的 → 动态队列分给不同源并行取
        3. 取到的写入磁盘缓存

        Args:
            market:         市场类型（兼容旧接口，新架构不使用）
            symbols:        股票代码列表
            timeframe:      周期
            limit:          数据条数
            cached_symbols: 已有缓存的 symbol 集合（兼容旧接口，新架构不使用）
        """
        if not symbols:
            return {}

        resolved_market = _resolve_market(market, symbols[0] if symbols else "")
        data_type = f"kline:{timeframe}"
        result = {}
        need_fetch = []

        # 1. 分离已缓存和需取的
        for sym in symbols:
            key = make_key("kline", sym, timeframe, limit)
            cached = self._cache.get(key, data_type)
            if cached is not None:
                result[sym] = cached
            else:
                need_fetch.append(sym)

        if not need_fetch:
            logger.info(f"[批量K线] 全部 {len(symbols)} 只命中缓存")
            for sym in result:
                result[sym] = adjust_kline(sym, result[sym], "qfq")
            return result

        logger.info(f"[批量K线] 缓存命中 {len(result)}/{len(symbols)}，需取 {len(need_fetch)}")

        # 2. 动态队列: 分源并行取（每个源干不同的活）
        providers = [
            (p.name, lambda s, p=p: p.fetch_kline(s, timeframe, limit))
            for p in get_providers("kline", timeframe=timeframe, market=resolved_market)
        ]

        fetched, failed = dynamic_queue(
            symbols=need_fetch, providers=providers, cb=self._cb,
            timeout=10.0,
            validate=lambda bars: bars and len(bars) > 0,
        )

        # 3. 写缓存（原始数据）+ 合并结果
        ttl = get_ttl("kline", timeframe)
        for sym, bars in fetched.items():
            key = make_key("kline", sym, timeframe, limit)
            self._cache.set(key, bars, ttl, data_type)
            result[sym] = bars

        if failed:
            logger.warning(f"[批量K线] {len(failed)} 只所有源均失败: {failed[:5]}{'...' if len(failed) > 5 else ''}")

        # 4. 对所有结果统一复权
        for sym in result:
            result[sym] = adjust_kline(sym, result[sym], "qfq")

        logger.info(f"[批量K线] 最终 {len(result)}/{len(symbols)}，失败 {len(failed)}")
        return result

    # ═══════════════════════════════════════════════════════════════════
    #  行情
    # ═══════════════════════════════════════════════════════════════════

    def get_ticker(self, market: str, symbol: str) -> Dict[str, Any]:
        """
        获取实时行情 — 内存缓存30s，去重，顺序fallback。

        Args:
            market: 市场类型（'CNStock'/'HKStock'/...，为空则自动推断）
            symbol: 股票代码
        """
        resolved_market = _resolve_market(market, symbol)
        key = make_key("ticker", symbol)

        # 1. 查缓存（内存）
        cached = self._cache.get(key, "ticker")
        if cached is not None:
            return cached

        # 2. 去重
        result = self._dedup.get_or_submit(
            key, lambda: self._fetch_ticker(symbol, resolved_market)
        )

        # 3. 写缓存（内存）
        if result and result.get("last", 0) > 0:
            self._cache.set(key, result, get_ttl("ticker"), "ticker")

        return result or {"last": 0, "symbol": symbol}

    def get_latest_price(self, market: str, symbol: str) -> Optional[Dict[str, Any]]:
        """获取最新价格"""
        klines = self.get_kline(market, symbol, "1m", 1)
        return klines[-1] if klines else None

    def get_realtime_price(
        self, market: str, symbol: str, force_refresh: bool = False,
    ) -> Dict[str, Any]:
        """获取实时价格（兼容旧接口）"""
        result = {
            'price': 0, 'change': 0, 'changePercent': 0,
            'high': 0, 'low': 0, 'open': 0, 'previousClose': 0, 'source': 'unknown'
        }

        try:
            ticker = self.get_ticker(market, symbol)
            if ticker and ticker.get('last', 0) > 0:
                return {
                    'price': ticker.get('last', 0),
                    'change': ticker.get('change', 0),
                    'changePercent': ticker.get('changePercent') or ticker.get('percentage', 0),
                    'high': ticker.get('high', 0), 'low': ticker.get('low', 0),
                    'open': ticker.get('open', 0), 'previousClose': ticker.get('previousClose', 0),
                    'source': 'ticker'
                }
        except Exception:
            pass

        try:
            klines = self.get_kline(market, symbol, '1D', 2)
            if klines and len(klines) > 0:
                latest = klines[-1]
                prev = klines[-2]['close'] if len(klines) > 1 else latest.get('open', 0)
                price = latest.get('close', 0)
                chg = round(price - prev, 4) if prev else 0
                pct = round(chg / prev * 100, 2) if prev and prev > 0 else 0
                return {
                    'price': price, 'change': chg, 'changePercent': pct,
                    'high': latest.get('high', 0), 'low': latest.get('low', 0),
                    'open': latest.get('open', 0), 'previousClose': prev,
                    'source': 'kline_1d'
                }
        except Exception:
            pass

        return result

    # ═══════════════════════════════════════════════════════════════════
    #  缓存管理
    # ═══════════════════════════════════════════════════════════════════

    def get_cache_dir(self) -> str:
        """获取缓存目录路径（供外部查询用）"""
        return str(self._cache.disk._base_dir)

    def invalidate(self, symbol: str = None, data_type: str = None) -> int:
        """
        清除缓存。

        用法:
            invalidate()                    # 清全部
            invalidate(data_type="kline")   # 清所有K线缓存（磁盘）
            invalidate(symbol="SH600519")   # 清某只股票的所有缓存
        """
        if symbol:
            market, digits = detect_market(symbol)
            count = 0

            # 内存: 遍历所有 key，找到包含该 symbol 的删掉
            with self._cache.memory._lock:
                keys_to_delete = [
                    k for k in self._cache.memory._store
                    if f":{symbol}:" in k or k.endswith(f":{symbol}")
                ]
            for k in keys_to_delete:
                self._cache.memory.delete(k)
                count += 1

            # 磁盘: 按 data_type 逐个清
            if digits:
                for dt in ("kline", "stock_info"):
                    self._cache.disk.delete(dt, market, digits)
                    count += 1

            return count
        if data_type:
            return self._cache.clear(data_type)
        return self._cache.clear()

    def cache_stats(self) -> Dict[str, Any]:
        """缓存统计"""
        return self._cache.stats()

    # ═══════════════════════════════════════════════════════════════════
    #  预热
    # ═══════════════════════════════════════════════════════════════════

    def prewarm_all(self, symbols: List[str], market: str = "CNStock") -> Dict[str, bool]:
        """
        统一预热入口 — 批量拉取K线写入缓存。

        Args:
            symbols: 股票代码列表
            market:  市场类型
        """
        results = {}
        try:
            fetched = self.get_kline_batch(market, symbols, "1D", 300)
            results["1D"] = len(fetched) > 0
            logger.info(f"[预热] {market} 1D: {len(fetched)}/{len(symbols)} 成功")
        except Exception as e:
            logger.warning(f"[预热] {market} 1D 失败: {e}")
            results["1D"] = False
        return results

    # ═══════════════════════════════════════════════════════════════════
    #  内部方法
    # ═══════════════════════════════════════════════════════════════════

    def _fetch_ticker(self, symbol: str, market: str = "") -> Optional[Dict[str, Any]]:
        """内部: 顺序fallback取行情"""
        providers = [
            (p.name, lambda p=p: p.fetch_quote(symbol))
            for p in get_providers("quote", market=market or None)
        ]
        result, src = sequential_fallback(symbol, providers, self._cb)
        if result:
            logger.info(f"[行情] {symbol} 来源={src}")
        else:
            names = [p.name for p in get_providers("quote", market=market or None)]
            logger.warning(f"[行情] {symbol} 全部源失败: {names}")
        return result

    def _fetch_kline(self, symbol: str, timeframe: str, limit: int, market: str = "") -> Optional[List]:
        """内部: 顺序fallback取K线"""
        providers = [
            (p.name, lambda p=p: p.fetch_kline(symbol, timeframe, limit))
            for p in get_providers("kline", timeframe=timeframe, market=market or None)
        ]
        result, src = sequential_fallback(symbol, providers, self._cb)
        if result:
            logger.info(f"[K线] {symbol} tf={timeframe} 来源={src} bars={len(result)}")
        else:
            names = [p.name for p in get_providers("kline", timeframe=timeframe, market=market or None)]
            logger.warning(f"[K线] {symbol} tf={timeframe} 全部源失败: {names}")
        return result
