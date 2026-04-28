# -*- coding: utf-8 -*-
"""
K线数据服务 — 缓存层

职责单一化:
  本层只负责缓存读写，所有数据获取逻辑委托给 DataSourceFactory。

架构:
  KlineService(缓存层) → DataSourceFactory(工厂层) → Coordinator(协助层) → Providers(数据源层)

调用链:
  KlineService.get_kline(symbol, tf, limit)
    → 查缓存 → 命中? adjust返回 : fetch_kline_raw → 写缓存 → adjust → 返回

  KlineService.get_ticker(symbol)
    → 查缓存 → 命中? 返回 : fetch_ticker → 写缓存 → 返回

  KlineService.get_kline_batch(symbols)
    → 逐只查缓存 → 分离已缓存/未缓存
    → fetch_kline_batch(未缓存的) → 写缓存 → 合并返回

两层存储:
  热数据（行情/分钟线） → 内存 dict，丢了30s重取
  温数据（K线/股票信息） → feather 文件，进程重启还在
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from app.data_sources.adjustment import adjust_kline
from app.data_sources.cache import get_cache, make_key, get_ttl
from app.data_sources.factory import (
    get_factory, _parse_symbols, _resolve_market, _normalize_symbols,
)
from app.utils.logger import get_logger

logger = get_logger(__name__)


class KlineService:
    """K线数据服务 — 缓存层，只负责缓存读写"""

    def __init__(self):
        self._cache = get_cache()
        self._factory = get_factory()

    # ═══════════════════════════════════════════════════════════════════
    #  K线
    # ═══════════════════════════════════════════════════════════════════

    def get_kline(
        self,
        market: str,
        symbol: str,
        timeframe: str,
        limit: int = 1000,
        before_time: Optional[int] = None,
        adj: str = "qfq",
    ) -> Any:
        """
        获取K线 — 支持单只或批量。

        单只: symbol="SH600519"          → 返回 List[Dict]
        批量: symbol="SH600519,SZ000001" → 返回 Dict[str, List[Dict]]

        流程:
          1. 查缓存(原始数据) → 命中 → 复权 → 返回
          2. 未命中 → fetch_kline_raw → 写缓存 → 复权 → 返回

        Args:
            market:    市场类型（兼容旧接口）
            symbol:    股票代码，支持逗号分隔批量: "A,B,C"
            timeframe: 周期 ('1m','5m','15m','30m','1H','1D','1W','1M')
            limit:     数据条数
            before_time: 分页用，获取此时间之前的数据（可选）
            adj:       复权方式 ('qfq'/'hfq'/''不复权)
        """
        # 批量模式 → 委托 get_kline_batch
        symbols = _parse_symbols(symbol)
        if len(symbols) > 1:
            resolved_market = _resolve_market(market, symbols[0])
            if not resolved_market:
                raise ValueError(
                    f"批量K线必须传 market 参数，无法从 '{symbols[0]}' 推断市场"
                )
            symbols = _normalize_symbols(symbols, resolved_market)
            return self.get_kline_batch(resolved_market, symbols, timeframe, limit)

        # 单只模式
        symbol = symbols[0] if symbols else symbol
        resolved_market = _resolve_market(market, symbol)

        # before_time 模式不缓存（历史翻页），直接穿透
        if before_time:
            raw = self._factory.fetch_kline_raw(
                symbol, timeframe, limit, resolved_market
            )
            return adjust_kline(symbol, raw or [], adj)

        # 1. 查缓存（原始数据）
        key = make_key("kline", symbol, timeframe, limit)
        data_type = f"kline:{timeframe}"
        cached = self._cache.get(key, data_type)
        if cached is not None:
            return adjust_kline(symbol, cached, adj)

        # 2. 缓存未命中 → DataSourceFactory 取原始数据
        raw = self._factory.fetch_kline_raw(
            symbol, timeframe, limit, resolved_market
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
        批量K线 — 缓存分离 + DataSourceFactory 协调。

        流程:
          1. 逐只查缓存（原始数据）→ 命中 → 复权 → 直接用
          2. 未命中的 → fetch_kline_batch（通过回调同步写缓存）
          3. 合并返回
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
                result[sym] = adjust_kline(sym, cached, "qfq")
            else:
                need_fetch.append(sym)

        if not need_fetch:
            logger.info(f"[批量K线] 全部 {len(symbols)} 只命中缓存")
            return result

        logger.info(
            f"[批量K线] 缓存命中 {len(result)}/{len(symbols)}，"
            f"需取 {len(need_fetch)}"
        )

        # 2. 缓存分离 + DataSourceFactory 批量获取
        #    on_raw_data 回调: 获取到原始数据时立即写缓存，避免二次获取
        ttl = get_ttl("kline", timeframe)

        def _write_raw_to_cache(sym: str, raw_bars: List[Dict[str, Any]]):
            """回调: Coordinator 获取到原始数据时写入缓存"""
            key = make_key("kline", sym, timeframe, limit)
            self._cache.set(key, raw_bars, ttl, data_type)

        fetched = self._factory.fetch_kline_batch(
            need_fetch, timeframe, limit, resolved_market,
            on_raw_data=_write_raw_to_cache,
        )

        # 3. 合并结果
        result.update(fetched)

        failed = [s for s in need_fetch if s not in fetched]
        if failed:
            logger.warning(
                f"[批量K线] {len(failed)} 只获取失败: "
                f"{failed[:5]}{'...' if len(failed) > 5 else ''}"
            )

        logger.info(
            f"[批量K线] 最终 {len(result)}/{len(symbols)}，失败 {len(failed)}"
        )
        return result

    # ═══════════════════════════════════════════════════════════════════
    #  行情
    # ═══════════════════════════════════════════════════════════════════

    def get_ticker(self, market: str, symbol: str) -> Any:
        """
        获取实时行情 — 支持单只或批量。

        单只: symbol="SH600519"          → 返回 Dict
        批量: symbol="SH600519,SZ000001" → 返回 Dict[str, Dict]

        流程:
          1. 查缓存（内存）→ 命中 → 返回
          2. 未命中 → DataSourceFactory.fetch_ticker → 写缓存 → 返回
        """
        # 批量模式
        symbols = _parse_symbols(symbol)
        if len(symbols) > 1:
            resolved_market = _resolve_market(market, symbols[0])
            if not resolved_market:
                raise ValueError(
                    f"批量行情必须传 market 参数，无法从 '{symbols[0]}' 推断市场"
                )
            symbols = _normalize_symbols(symbols, resolved_market)
            return self._get_ticker_batch(resolved_market, symbols)

        # 单只模式
        symbol = symbols[0] if symbols else symbol
        resolved_market = _resolve_market(market, symbol)

        # 1. 查缓存（内存）
        key = make_key("ticker", symbol)
        cached = self._cache.get(key, "ticker")
        if cached is not None:
            return cached

        # 2. 缓存未命中 → DataSourceFactory 取数据
        result = self._factory.fetch_ticker(symbol, resolved_market)

        # 3. 写缓存（内存）
        if result and result.get("last", 0) > 0:
            self._cache.set(key, result, get_ttl("ticker"), "ticker")

        return result or {"last": 0, "symbol": symbol}

    def _get_ticker_batch(
        self, market: str, symbols: List[str],
    ) -> Dict[str, Dict[str, Any]]:
        """
        批量行情 — 缓存分离 + DataSourceFactory 批量获取。

        流程:
          1. 逐只查内存缓存 → 命中直接用
          2. 未命中的 → DataSourceFactory.fetch_ticker_batch
          3. 写缓存 → 返回
        """
        result: Dict[str, Dict[str, Any]] = {}
        need_fetch: List[str] = []

        # 1. 分离已缓存和需取的
        for sym in symbols:
            key = make_key("ticker", sym)
            cached = self._cache.get(key, "ticker")
            if cached is not None:
                result[sym] = cached
            else:
                need_fetch.append(sym)

        if not need_fetch:
            logger.info(f"[批量行情] 全部 {len(symbols)} 只命中缓存")
            return result

        logger.info(
            f"[批量行情] 缓存命中 {len(result)}/{len(symbols)}，"
            f"需取 {len(need_fetch)}"
        )

        # 2. DataSourceFactory 批量获取
        fetched = self._factory.fetch_ticker_batch(need_fetch, market)

        # 3. 写缓存 + 合并结果
        for sym, data in fetched.items():
            if data and data.get("last", 0) > 0:
                key = make_key("ticker", sym)
                self._cache.set(key, data, get_ttl("ticker"), "ticker")
                result[sym] = data

        failed = [s for s in need_fetch if s not in result]
        if failed:
            logger.warning(
                f"[批量行情] {len(failed)} 只获取失败: "
                f"{failed[:5]}{'...' if len(failed) > 5 else ''}"
            )

        logger.info(
            f"[批量行情] 最终 {len(result)}/{len(symbols)}，失败 {len(failed)}"
        )
        return result

    # ═══════════════════════════════════════════════════════════════════
    #  便捷方法
    # ═══════════════════════════════════════════════════════════════════

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
            'high': 0, 'low': 0, 'open': 0, 'previousClose': 0,
            'source': 'unknown',
        }

        try:
            ticker = self.get_ticker(market, symbol)
            if ticker and ticker.get('last', 0) > 0:
                return {
                    'price': ticker.get('last', 0),
                    'change': ticker.get('change', 0),
                    'changePercent': (
                        ticker.get('changePercent')
                        or ticker.get('percentage', 0)
                    ),
                    'high': ticker.get('high', 0),
                    'low': ticker.get('low', 0),
                    'open': ticker.get('open', 0),
                    'previousClose': ticker.get('previousClose', 0),
                    'source': 'ticker',
                }
        except Exception:
            pass

        try:
            klines = self.get_kline(market, symbol, '1D', 2)
            if klines and len(klines) > 0:
                latest = klines[-1]
                prev = (
                    klines[-2]['close']
                    if len(klines) > 1
                    else latest.get('open', 0)
                )
                price = latest.get('close', 0)
                chg = round(price - prev, 4) if prev else 0
                pct = round(chg / prev * 100, 2) if prev and prev > 0 else 0
                return {
                    'price': price, 'change': chg, 'changePercent': pct,
                    'high': latest.get('high', 0),
                    'low': latest.get('low', 0),
                    'open': latest.get('open', 0),
                    'previousClose': prev,
                    'source': 'kline_1d',
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
            from app.data_sources.normalizer import detect_market
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

    def source_stats(self) -> Dict[str, Any]:
        """各数据源吞吐统计（委托给 DataSourceFactory）"""
        return self._factory.source_stats()

    # ═══════════════════════════════════════════════════════════════════
    #  预热
    # ═══════════════════════════════════════════════════════════════════

    def prewarm_all(
        self, symbols: List[str], market: str = "CNStock",
    ) -> Dict[str, bool]:
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
            logger.info(
                f"[预热] {market} 1D: {len(fetched)}/{len(symbols)} 成功"
            )
        except Exception as e:
            logger.warning(f"[预热] {market} 1D 失败: {e}")
            results["1D"] = False
        return results
