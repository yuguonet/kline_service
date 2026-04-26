# -*- coding: utf-8 -*-
"""
东方财富数据源 Provider

能力: K线(全周期) / 单只行情 / 批量行情(全市场)
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

import requests

from app.data_sources.normalizer import to_eastmoney_secid, to_raw_digits, safe_float, safe_int
from app.data_sources.rate_limiter import (
    get_request_headers, retry_with_backoff, get_eastmoney_limiter,
)
from app.data_sources.provider import register
from app.utils.logger import get_logger

logger = get_logger(__name__)

_EM_KLT = {"1m": 1, "5m": 5, "15m": 15, "30m": 30, "1H": 60, "1D": 101, "1W": 102}
_EM_FQT = {"": 0, "qfq": 1, "hfq": 2}


@register(priority=30)
class EastMoneyDataSource:
    """东方财富 — 国内最稳定的免费数据源之一"""

    name = "eastmoney"
    priority = 30

    capabilities = {
        "kline": True,
        "kline_tf": {"1m", "5m", "15m", "30m", "1H", "1D", "1W"},
        "kline_batch": False,   # 东财K线API是per-symbol，无原生批量
        "quote": True,
        "batch_quote": True,
        "hk": False,
        "markets": {"CNStock"},
    }

    @retry_with_backoff(max_attempts=3, base_delay=2.0, max_delay=12.0, exceptions=(
        requests.exceptions.RequestException, ConnectionError, TimeoutError,
    ))
    def fetch_kline(
        self, code: str, timeframe: str = "1D", count: int = 300,
        adj: str = "qfq", timeout: int = 10,
    ) -> List[Dict[str, Any]]:
        secid = to_eastmoney_secid(code)
        if not secid:
            return []
        klt = _EM_KLT.get(timeframe)
        if klt is None:
            return []
        get_eastmoney_limiter().wait()
        resp = requests.get(
            "https://49.push2his.eastmoney.com/api/qt/stock/kline/get",
            headers=get_request_headers(referer="https://quote.eastmoney.com/"),
            params={
                "secid": secid, "ut": "fa5fd1943c7b386f172d6893dbbd1835",
                "fields1": "f1,f2,f3",
                "fields2": "f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61",
                "klt": klt, "fqt": 0,
                "end": "20500101", "lmt": min(int(count), 5000),
            },
            timeout=timeout,
        )
        try:
            data = resp.json()
        except Exception:
            return []
        if not isinstance(data, dict) or int(data.get("code", -1)) != 0:
            return []
        klines_data = (data.get("data") or {}).get("klines")
        if not isinstance(klines_data, list):
            return []
        out = []
        for line in klines_data:
            parts = line.split(",")
            if len(parts) < 7:
                continue
            try:
                dt_str = parts[0].strip()
                ts = None
                for fmt in ("%Y-%m-%d %H:%M", "%Y-%m-%d"):
                    try:
                        ts = int(datetime.strptime(dt_str, fmt).timestamp())
                        break
                    except ValueError:
                        continue
                if ts is None:
                    continue
                o, c, h, low, v = float(parts[1]), float(parts[2]), float(parts[3]), float(parts[4]), float(parts[5])
                if o == 0 and c == 0:
                    continue
                if h > 0 and low > 0 and h < low:
                    h, low = low, h
                out.append({
                    "time": ts, "open": round(o, 4), "high": round(h, 4),
                    "low": round(low, 4), "close": round(c, 4), "volume": round(v, 2),
                })
            except (ValueError, TypeError, IndexError):
                continue
        out.sort(key=lambda x: x["time"])
        return out[-count:] if len(out) > count else out

    def fetch_quote(self, code: str, timeout: int = 8) -> Optional[Dict[str, Any]]:
        secid = to_eastmoney_secid(code)
        if not secid:
            return None
        get_eastmoney_limiter().wait()
        resp = requests.get(
            "https://push2.eastmoney.com/api/qt/stock/get",
            headers=get_request_headers(referer="https://quote.eastmoney.com/"),
            params={
                "secid": secid,
                "ut": "fa5fd1943c7b386f172d6893dbfba10b",
                "fields": "f43,f44,f45,f46,f47,f48,f57,f58,f60,f170,f171",
            },
            timeout=timeout,
        )
        try:
            data = resp.json()
        except Exception:
            return None
        if not isinstance(data, dict) or int(data.get("code", -1)) != 0:
            return None
        d = data.get("data")
        if not isinstance(d, dict):
            return None
        def _f(key: str, default: float = 0.0) -> float:
            v = d.get(key)
            if v is None or v == "-" or v == "":
                return default
            try:
                return float(v)
            except (TypeError, ValueError):
                return default
        last = _f("f43")
        prev = _f("f60")
        if last == 0 and prev == 0:
            return None
        change = round(last - prev, 4) if prev else 0.0
        change_pct = round(change / prev * 100, 2) if prev else 0.0
        return {
            "symbol": secid, "name": str(d.get("f58", "")).strip(),
            "last": last, "change": change, "changePercent": change_pct,
            "high": _f("f44"), "low": _f("f45"), "open": _f("f46"),
            "previousClose": prev, "volume": _f("f47"), "amount": _f("f48"),
        }

    def fetch_quotes_batch(self, codes: List[str], timeout: int = 15) -> Dict[str, Dict[str, Any]]:
        if not codes:
            return {}
        code_set: Dict[str, str] = {}
        for sym in codes:
            raw = to_raw_digits(sym)
            if raw and raw.isdigit() and len(raw) == 6:
                code_set[raw] = sym
        if not code_set:
            return {}
        try:
            get_eastmoney_limiter().wait()
            resp = requests.get(
                "https://push2.eastmoney.com/api/qt/clist/get",
                headers=get_request_headers(referer="https://quote.eastmoney.com/"),
                params={
                    "pn": 1, "pz": 6000, "po": 1, "np": 1,
                    "ut": "bd1d9ddb04089700cf9c27f6f7426281",
                    "fltt": 2, "invt": 2, "fid": "f3",
                    "fs": "m:0+t:6,m:0+t:80,m:1+t:2,m:1+t:23",
                    "fields": "f2,f5,f6,f12,f15,f16,f17,f18",
                },
                timeout=timeout,
            )
            data = resp.json()
            diff = ((data.get("data") or {}).get("diff")) or []
        except Exception as e:
            logger.warning(f"[东财批量行情] clist 请求失败: {e}")
            return {}
        now = datetime.now(timezone(timedelta(hours=8)))
        today_ts = int(now.replace(hour=0, minute=0, second=0, microsecond=0).timestamp())
        result: Dict[str, Dict[str, Any]] = {}
        for item in diff:
            code = str(item.get("f12", "")).strip()
            sym = code_set.get(code)
            if not sym:
                continue
            try:
                last = float(item.get("f2", 0))
                if last <= 0:
                    continue
                result[sym] = {
                    "last": last,
                    "open": round(float(item.get("f17", 0)), 4),
                    "high": round(float(item.get("f15", 0)), 4),
                    "low": round(float(item.get("f16", 0)), 4),
                    "previousClose": float(item.get("f18", 0)),
                    "volume": round(float(item.get("f5", 0)), 2),
                    "name": "", "symbol": sym, "time": today_ts,
                }
            except (ValueError, TypeError):
                continue
        return result


# ================================================================
# 市场数据 — 独立函数
# ================================================================

def _em_request(report_name: str, params: dict = None, timeout: int = 10) -> list:
    get_eastmoney_limiter().wait()
    default_params = {
        "sortColumns": "TRADE_DATE", "sortTypes": "-1",
        "pageSize": 500, "pageNumber": 1,
        "reportName": report_name, "columns": "ALL",
        "source": "WEB", "client": "WEB",
    }
    if params:
        default_params.update(params)
    resp = requests.get(
        "https://datacenter-web.eastmoney.com/api/data/v1/get",
        headers=get_request_headers(referer="https://data.eastmoney.com/"),
        params=default_params, timeout=timeout,
    )
    try:
        data = resp.json()
    except Exception:
        return []
    return ((data.get("result") or {}).get("data")) or []

def fetch_dragon_tiger(start_date: str, end_date: str) -> List[Dict[str, Any]]:
    items = _em_request(
        "RPT_DAILYBILLBOARD_DETAILSNEW",
        params={"filter": f"(TRADE_DATE>='{start_date}')(TRADE_DATE<='{end_date}')"},
    )
    if not items:
        return []
    result = []
    for item in items:
        try:
            result.append({
                "stock_code": str(item.get("SECURITY_CODE", "")).strip(),
                "stock_name": str(item.get("SECURITY_NAME_ABBR", "")).strip(),
                "trade_date": (str(item.get("TRADE_DATE", ""))[:10]).strip(),
                "reason": str(item.get("EXPLANATION", "") or "").strip()[:100],
                "buy_amount": safe_float(item.get("BUY")),
                "sell_amount": safe_float(item.get("SELL")),
                "net_amount": safe_float(item.get("NET_BUY")),
                "change_percent": safe_float(item.get("CHANGE_RATE")),
                "close_price": safe_float(item.get("CLOSE_PRICE")),
                "turnover_rate": safe_float(item.get("TURNOVERRATE")),
                "amount": safe_float(item.get("ACCUM_AMOUNT")),
                "buy_seat_count": safe_int(item.get("BUYER_NUM") or 0),
                "sell_seat_count": safe_int(item.get("SELLER_NUM") or 0),
            })
        except Exception:
            continue
    return result

def fetch_hot_rank() -> List[Dict[str, Any]]:
    items = _em_request(
        "RPT_HOT_STOCK_NEW",
        params={
            "sortColumns": "CHANGE_RATE", "sortTypes": "-1", "pageSize": 50,
            "filter": "(MARKET_TYPE in (\"沪深A股\"))",
        },
    )
    if not items:
        return []
    result = []
    for i, item in enumerate(items):
        try:
            code = str(item.get("SECURITY_CODE", "")).strip()
            if not code:
                continue
            result.append({
                "rank": i + 1, "stock_code": code,
                "stock_name": str(item.get("SECURITY_NAME_ABBR", "")).strip(),
                "price": safe_float(item.get("NEWEST_PRICE", item.get("CLOSE_PRICE"))),
                "change_percent": safe_float(item.get("CHANGE_RATE")),
                "popularity_score": safe_float(item.get("HOT_NUM", item.get("SCORE"))),
                "current_rank_change": str(item.get("RANK_CHANGE", "")),
            })
        except Exception:
            continue
    return result

def fetch_zt_pool(trade_date: str) -> List[Dict[str, Any]]:
    items = _em_request(
        "RPT_LIMITED_BOARD_POOL",
        params={"sortColumns": "TOTAL_MARKET_CAP", "sortTypes": "-1",
                "filter": f"(TRADE_DATE='{trade_date}')"},
    )
    if not items:
        return []
    result = []
    for item in items:
        try:
            result.append({
                "stock_code": str(item.get("SECURITY_CODE", "")).strip(),
                "stock_name": str(item.get("SECURITY_NAME_ABBR", "")).strip(),
                "trade_date": trade_date,
                "price": safe_float(item.get("CLOSE_PRICE")),
                "change_percent": safe_float(item.get("CHANGE_RATE")),
                "continuous_zt_days": safe_int(item.get("CONTINUOUS_LIMIT_DAYS", item.get("ZT_DAYS", 1)) or 1),
                "zt_time": str(item.get("FIRST_ZDT_TIME", "")),
                "seal_amount": safe_float(item.get("LIMIT_ORDER_AMT")),
                "turnover_rate": safe_float(item.get("TURNOVERRATE")),
                "volume": safe_float(item.get("VOLUME")),
                "amount": safe_float(item.get("TURNOVER")),
                "sector": str(item.get("BOARD_NAME", "")),
                "reason": str(item.get("ZT_REASON", ""))[:80],
                "open_count": safe_int(item.get("OPEN_NUM", 0) or 0),
            })
        except Exception:
            continue
    return result

def fetch_dt_pool(trade_date: str) -> List[Dict[str, Any]]:
    items = _em_request(
        "RPT_DOWNTREND_LIMIT_POOL",
        params={"sortColumns": "TOTAL_MARKET_CAP", "sortTypes": "-1",
                "filter": f"(TRADE_DATE='{trade_date}')"},
    )
    if not items:
        return []
    result = []
    for item in items:
        try:
            result.append({
                "stock_code": str(item.get("SECURITY_CODE", "")).strip(),
                "stock_name": str(item.get("SECURITY_NAME_ABBR", "")).strip(),
                "trade_date": trade_date,
                "price": safe_float(item.get("CLOSE_PRICE")),
                "change_percent": safe_float(item.get("CHANGE_RATE")),
                "seal_amount": safe_float(item.get("LIMIT_ORDER_AMT")),
                "turnover_rate": safe_float(item.get("TURNOVERRATE")),
                "amount": safe_float(item.get("TURNOVER")),
            })
        except Exception:
            continue
    return result

def fetch_broken_board(trade_date: str) -> List[Dict[str, Any]]:
    items = _em_request(
        "RPT_LIMITED_BOARD_UNSEALED",
        params={"sortColumns": "TOTAL_MARKET_CAP", "sortTypes": "-1",
                "filter": f"(TRADE_DATE='{trade_date}')"},
    )
    if not items:
        return []
    result = []
    for item in items:
        try:
            result.append({
                "stock_code": str(item.get("SECURITY_CODE", "")).strip(),
                "stock_name": str(item.get("SECURITY_NAME_ABBR", "")).strip(),
                "trade_date": trade_date,
                "price": safe_float(item.get("CLOSE_PRICE")),
                "change_percent": safe_float(item.get("CHANGE_RATE")),
                "zt_time": str(item.get("FIRST_ZDT_TIME", "")),
                "break_time": str(item.get("LAST_ZDT_TIME", "")),
                "turnover_rate": safe_float(item.get("TURNOVERRATE")),
                "amount": safe_float(item.get("TURNOVER")),
            })
        except Exception:
            continue
    return result

def aggregate_daily_to_monthly(daily_bars: List[Dict[str, Any]], limit: int) -> List[Dict[str, Any]]:
    if not daily_bars:
        return []
    bars = sorted(daily_bars, key=lambda x: x.get("time", 0))
    groups: Dict[int, list] = {}
    order: List[int] = []
    for bar in bars:
        t = bar.get("time", 0)
        if not t:
            continue
        dt = datetime.fromtimestamp(t, tz=timezone(timedelta(hours=8)))
        ms = int(dt.replace(day=1, hour=0, minute=0, second=0, microsecond=0).timestamp())
        if ms not in groups:
            groups[ms] = []
            order.append(ms)
        groups[ms].append(bar)
    result = []
    for ms in order:
        chunk = groups[ms]
        if not chunk:
            continue
        result.append({
            "time": ms,
            "open": float(chunk[0].get("open", 0)),
            "high": max(float(b.get("high", 0)) for b in chunk),
            "low": min(float(b.get("low", 0)) for b in chunk),
            "close": float(chunk[-1].get("close", 0)),
            "volume": round(sum(float(b.get("volume", 0)) for b in chunk), 2),
        })
    return result[-limit:] if len(result) > limit else result
