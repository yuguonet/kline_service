# -*- coding: utf-8 -*-
"""
复权因子计算 — 上层统一复权

所有 Provider 返回原始(未复权)数据，复权在这里统一处理。
除权除息数据从东财获取，缓存 24h。

用法:
    from app.data_sources.adjustment import adjust_kline
    raw_bars = provider.fetch_kline(code, tf, limit, adj="")  # 原始
    adjusted = adjust_kline(code, raw_bars, adj="qfq")         # 复权
"""

from __future__ import annotations

import threading
import time
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

import requests

from app.data_sources.normalizer import to_raw_digits
from app.data_sources.rate_limiter import (
    get_request_headers, get_eastmoney_limiter,
)
from app.utils.logger import get_logger

logger = get_logger(__name__)


# ================================================================
# 除权除息数据 — 从东财获取，缓存 24h
# ================================================================

_exdiv_cache: Dict[str, Tuple[List[Dict], float]] = {}
_exdiv_cache_lock = threading.Lock()
_EXDIV_CACHE_TTL = 86400

def fetch_exdividend_events(code: str) -> List[Dict[str, Any]]:
    """
    获取股票的除权除息记录（东财 API）。

    返回按除权日升序排列的事件列表。
    结果缓存 24h — 除权除息数据一年才变几次。
    """
    now = time.time()
    raw_code = to_raw_digits(code)
    if not raw_code or not raw_code.isdigit() or len(raw_code) != 6:
        return []

    with _exdiv_cache_lock:
        if raw_code in _exdiv_cache:
            events, cached_ts = _exdiv_cache[raw_code]
            if now - cached_ts < _EXDIV_CACHE_TTL:
                return events

    try:
        get_eastmoney_limiter().wait()
        resp = requests.get(
            "https://datacenter-web.eastmoney.com/api/data/v1/get",
            headers=get_request_headers(referer="https://data.eastmoney.com/"),
            params={
                "sortColumns": "EX_DIVIDEND_DATE",
                "sortTypes": "-1",
                "pageSize": 200,
                "pageNumber": 1,
                "reportName": "RPT_SHAREBONUS_DET",
                "columns": "ALL",
                "source": "WEB",
                "client": "WEB",
                "filter": f'(SECURITY_CODE="{raw_code}")',
            },
            timeout=10,
        )
        data = resp.json()
        items = ((data.get("result") or {}).get("data")) or []
    except Exception as e:
        logger.warning(f"[复权] 获取除权除息数据失败 {code}: {e}")
        return []

    events = []
    for item in items:
        ex_date = str(item.get("EX_DIVIDEND_DATE", "")).strip()[:10]
        if not ex_date:
            continue

        bonus_cash = 0.0
        for key in ("PRETAX_BONUS_RMB", "BONUS_CASH", "DIVIDEND_AMOUNT"):
            v = item.get(key)
            if v is not None and v != "-" and v != "":
                try:
                    bonus_cash = float(v)
                    break
                except (TypeError, ValueError):
                    continue

        bonus_shares = 0.0
        for key in ("BONUS_SHARES_RATIO", "BONUS_RATIO"):
            v = item.get(key)
            if v is not None and v != "-" and v != "":
                try:
                    bonus_shares = float(v)
                    break
                except (TypeError, ValueError):
                    continue

        rights_shares = 0.0
        v = item.get("RIGHTS_SHARES_RATIO")
        if v is not None and v != "-" and v != "":
            try:
                rights_shares = float(v)
            except (TypeError, ValueError):
                pass

        rights_price = 0.0
        v = item.get("RIGHTS_SHARES_PRICE")
        if v is not None and v != "-" and v != "":
            try:
                rights_price = float(v)
            except (TypeError, ValueError):
                pass

        if bonus_cash > 0 or bonus_shares > 0 or rights_shares > 0:
            events.append({
                "date": ex_date,
                "bonus_cash": bonus_cash,
                "bonus_shares": bonus_shares,
                "rights_shares": rights_shares,
                "rights_price": rights_price,
            })

    events.sort(key=lambda x: x["date"])

    with _exdiv_cache_lock:
        _exdiv_cache[raw_code] = (events, time.time())

    if events:
        logger.debug(f"[复权] {code} 获取 {len(events)} 条除权除息记录")
    return events


# ================================================================
# 复权因子计算
# ================================================================

def calc_adjustment_factors(
    kline: List[Dict[str, Any]],
    events: List[Dict[str, Any]],
    adj: str = "qfq",
) -> Dict[int, float]:
    """
    计算复权因子，返回 {timestamp: factor}。

    前复权 (qfq): 最新价不变，历史价下调
    后复权 (hfq): 最早价不变，新价上调
    """
    if not events or not kline:
        return {}

    date_to_close: Dict[str, float] = {}
    for bar in kline:
        ts = bar.get("time", 0)
        if ts and bar.get("close", 0) > 0:
            dt = datetime.fromtimestamp(ts).strftime("%Y-%m-%d")
            date_to_close[dt] = bar["close"]

    def _get_prev_close(ex_date: str) -> Optional[float]:
        dt = datetime.strptime(ex_date, "%Y-%m-%d")
        for i in range(1, 15):
            prev_str = (dt - timedelta(days=i)).strftime("%Y-%m-%d")
            c = date_to_close.get(prev_str)
            if c and c > 0:
                return c
        return None

    event_pairs: List[Tuple[str, float]] = []
    for ev in events:
        prev_close = _get_prev_close(ev["date"])
        if prev_close is None or prev_close <= 0:
            continue

        bonus_cash = ev["bonus_cash"]
        bonus_shares = ev["bonus_shares"]
        rights_shares = ev["rights_shares"]
        rights_price = ev["rights_price"]

        denom = 1.0 + bonus_shares + rights_shares
        if denom <= 0:
            continue
        ref_price = (prev_close - bonus_cash + rights_price * rights_shares) / denom
        factor = ref_price / prev_close

        if 0.01 < factor < 100:
            event_pairs.append((ev["date"], factor))

    if not event_pairs:
        return {}

    event_pairs.sort(key=lambda x: x[0])
    result: Dict[int, float] = {}

    if adj == "qfq":
        cum = 1.0
        factor_map: Dict[str, float] = {}
        for ex_date, factor in reversed(event_pairs):
            cum *= factor
            factor_map[ex_date] = cum

        active = 1.0
        rev_idx = len(event_pairs) - 1
        for bar in reversed(kline):
            ts = bar.get("time", 0)
            if not ts:
                continue
            dt = datetime.fromtimestamp(ts).strftime("%Y-%m-%d")
            while rev_idx >= 0:
                ex_date, _ = event_pairs[rev_idx]
                if dt < ex_date:
                    active = factor_map[ex_date]
                    rev_idx -= 1
                else:
                    break
            result[ts] = active
    else:
        cum = 1.0
        factor_map: Dict[str, float] = {}
        for ex_date, factor in event_pairs:
            cum *= (1.0 / factor)
            factor_map[ex_date] = cum

        active_factor = 1.0
        event_idx = 0
        for bar in kline:
            ts = bar.get("time", 0)
            if not ts:
                continue
            dt = datetime.fromtimestamp(ts).strftime("%Y-%m-%d")
            while event_idx < len(event_pairs):
                ex_date, _ = event_pairs[event_idx]
                if dt >= ex_date:
                    active_factor = factor_map[ex_date]
                    event_idx += 1
                else:
                    break
            result[ts] = active_factor

    return result


# ================================================================
# 上层统一入口
# ================================================================

def adjust_kline(
    code: str,
    raw_bars: List[Dict[str, Any]],
    adj: str = "qfq",
) -> List[Dict[str, Any]]:
    """
    对原始K线应用复权。

    Args:
        code:      股票代码 (任意格式)
        raw_bars:  Provider 返回的原始(未复权)K线
        adj:       "qfq" / "hfq" / "" (不复权)

    Returns:
        复权后的K线。如果无除权除息记录，返回原始数据。
    """
    if not adj or not raw_bars:
        return raw_bars

    events = fetch_exdividend_events(code)
    if not events:
        return raw_bars

    factors = calc_adjustment_factors(raw_bars, events, adj)
    if not factors:
        return raw_bars

    adjusted = []
    for bar in raw_bars:
        ts = bar.get("time", 0)
        factor = factors.get(ts, 1.0)
        if factor == 1.0:
            adjusted.append(bar)
        else:
            adjusted.append({
                "time": ts,
                "open": round(bar["open"] * factor, 4),
                "high": round(bar["high"] * factor, 4),
                "low": round(bar["low"] * factor, 4),
                "close": round(bar["close"] * factor, 4),
                "volume": bar["volume"],
            })

    return adjusted
