# -*- coding: utf-8 -*-
"""
AkShare 数据源 Provider — A股国内兜底

能力: K线(日/周/分钟) / 批量行情
特点: 国内直连、免费、数据全
限制: 延迟 import akshare（重量级依赖），限流严格
定位: 东财之后的兜底源 (priority=50)
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

from app.data_sources.normalizer import to_raw_digits
from app.data_sources.rate_limiter import (
    get_request_headers, retry_with_backoff, RateLimiter,
)
from app.data_sources.provider import register
from app.utils.logger import get_logger

logger = get_logger(__name__)


# ---------- 限流器 ----------

_akshare_limiter = RateLimiter(
    min_interval=1.0,
    jitter_min=0.5,
    jitter_max=2.0,
)


# ---------- akshare 延迟导入 ----------

_ak = None
_ak_imported = False

def _get_ak():
    global _ak, _ak_imported
    if _ak_imported:
        return _ak
    _ak_imported = True
    try:
        import akshare as ak
        _ak = ak
        logger.info("[AkShare] 导入成功")
        return ak
    except ImportError:
        logger.debug("[AkShare] akshare 未安装，跳过")
        return None
    except Exception as e:
        logger.warning(f"[AkShare] 导入失败: {e}")
        return None


# ---------- 内部工具 ----------

_AK_PERIOD_MAP = {
    "1m": "1", "5m": "5", "15m": "15", "30m": "30", "60m": "60",
    "1D": "daily", "1W": "weekly",
}

_AK_ADJ_MAP = {"": "", "qfq": "qfq", "hfq": "hfq"}


def _parse_ak_kline(df, count: int) -> List[Dict[str, Any]]:
    import math
    if df is None:
        return []
    if not hasattr(df, "empty") or not hasattr(df, "columns"):
        return []
    if df.empty:
        return []
    out = []
    col_map = {}
    cols = [str(c).lower() for c in df.columns]
    for orig, candidates in {
        "time": ["日期", "date", "时间"],
        "open": ["开盘", "open"],
        "high": ["最高", "high"],
        "low": ["最低", "low"],
        "close": ["收盘", "close"],
        "volume": ["成交量", "volume"],
    }.items():
        for c in candidates:
            if c in cols:
                col_map[orig] = df.columns[cols.index(c)]
                break
    if "time" not in col_map or "close" not in col_map:
        return []

    def _safe_float(val, default=0.0):
        try:
            v = float(val)
            if math.isnan(v) or math.isinf(v):
                return default
            return v
        except (TypeError, ValueError):
            return default

    for _, row in df.iterrows():
        try:
            dt_val = row[col_map["time"]]
            if isinstance(dt_val, str):
                ts = None
                for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d"):
                    try:
                        ts = int(datetime.strptime(dt_val, fmt).timestamp())
                        break
                    except ValueError:
                        continue
                if ts is None:
                    continue
            elif hasattr(dt_val, "timestamp"):
                ts = int(dt_val.timestamp())
            else:
                ts = int(dt_val)
            o = _safe_float(row.get(col_map.get("open", "")))
            h = _safe_float(row.get(col_map.get("high", "")))
            low = _safe_float(row.get(col_map.get("low", "")))
            c = _safe_float(row[col_map["close"]])
            v = _safe_float(row.get(col_map.get("volume", "")))
            if o == 0 and c == 0:
                continue
            if h > 0 and low > 0 and h < low:
                h, low = low, h
            out.append({
                "time": ts, "open": round(o, 4), "high": round(h, 4),
                "low": round(low, 4), "close": round(c, 4), "volume": round(v, 2),
            })
        except (ValueError, TypeError, KeyError):
            continue
    out.sort(key=lambda x: x["time"])
    return out[-count:] if len(out) > count else out


@register(priority=50)
class AkShareDataSource:
    """AkShare — A股国内兜底数据源"""

    name = "akshare"
    priority = 50

    capabilities = {
        "kline": True,
        "kline_tf": {"1m", "5m", "15m", "30m", "1H", "1D", "1W"},
        "quote": False,
        "batch_quote": True,
        "hk": False,
        "markets": {"CNStock", "HKStock"},
    }

    def fetch_kline(
        self, code: str, timeframe: str = "1D", count: int = 300,
        adj: str = "qfq", timeout: int = 10,
    ) -> List[Dict[str, Any]]:
        ak = _get_ak()
        if not ak:
            return []
        raw_code = to_raw_digits(code)
        if not raw_code:
            return []
        lim = max(int(count or 300), 1)
        try:
            if timeframe in ("1D", "1W"):
                return self._fetch_daily_weekly(ak, raw_code, timeframe, lim, adj)
            else:
                return self._fetch_minute(ak, raw_code, timeframe, lim)
        except Exception as e:
            logger.warning(f"[AkShare] K线失败 {code} tf={timeframe}: {e}")
            return []

    def _fetch_daily_weekly(self, ak, code: str, timeframe: str, count: int, adj: str) -> List[Dict[str, Any]]:
        _akshare_limiter.wait()
        period = "daily" if timeframe == "1D" else "weekly"
        adjust = _AK_ADJ_MAP.get(adj, "qfq")
        end_date = datetime.now().strftime("%Y%m%d")
        days_back = count * (7 if timeframe == "1W" else 2)
        start_date = (datetime.now() - timedelta(days=days_back)).strftime("%Y%m%d")
        try:
            df = ak.stock_zh_a_hist(
                symbol=code, period=period,
                start_date=start_date, end_date=end_date, adjust=adjust,
            )
        except TypeError:
            df = ak.stock_zh_a_hist(
                symbol=code, period=period,
                start_date=start_date, end_date=end_date,
            )
        return _parse_ak_kline(df, count)

    def _fetch_minute(self, ak, code: str, timeframe: str, count: int) -> List[Dict[str, Any]]:
        _akshare_limiter.wait()
        ak_period_map = {"1m": "1", "5m": "5", "15m": "15", "30m": "30", "1H": "60"}
        period = ak_period_map.get(timeframe)
        if not period:
            return []
        if not hasattr(ak, "stock_zh_a_hist_min_em"):
            return []
        try:
            df = ak.stock_zh_a_hist_min_em(
                symbol=code, period=period,
                start_date=(datetime.now() - timedelta(days=5)).strftime("%Y-%m-%d %H:%M:%S"),
                end_date=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            )
        except TypeError:
            df = ak.stock_zh_a_hist_min_em(symbol=code, period=period)
        return _parse_ak_kline(df, count)

    def fetch_quote(self, code: str, timeout: int = 8) -> Optional[Dict[str, Any]]:
        return None

    def fetch_quotes_batch(self, codes: List[str], timeout: int = 15) -> Dict[str, Dict[str, Any]]:
        import math
        ak = _get_ak()
        if not ak or not codes:
            return {}
        code_set: Dict[str, str] = {}
        for sym in codes:
            raw = to_raw_digits(sym)
            if raw and len(raw) == 6:
                code_set[raw] = sym
        if not code_set:
            return {}
        try:
            _akshare_limiter.wait()
            df = ak.stock_zh_a_spot_em()
            if df is None or not hasattr(df, "empty") or df.empty:
                return {}
        except Exception as e:
            logger.warning(f"[AkShare] 全市场行情失败: {e}")
            return {}
        cols = [str(c) for c in df.columns]
        code_col = None
        for c in ["代码", "code", "symbol"]:
            if c in cols:
                code_col = c
                break
        if not code_col:
            return {}
        def _find_col(candidates):
            for c in candidates:
                if c in cols:
                    return c
            return None
        def _safe_float(val, default=0.0):
            try:
                v = float(val)
                if math.isnan(v) or math.isinf(v):
                    return default
                return v
            except (TypeError, ValueError):
                return default
        last_col = _find_col(["最新价", "close", "price"])
        name_col = _find_col(["名称", "name"])
        open_col = _find_col(["今开", "open"])
        high_col = _find_col(["最高", "high"])
        low_col = _find_col(["最低", "low"])
        prev_col = _find_col(["昨收", "pre_close", "prev_close"])
        vol_col = _find_col(["成交量", "volume"])
        if not last_col:
            return {}
        now = datetime.now(timezone(timedelta(hours=8)))
        today_ts = int(now.replace(hour=0, minute=0, second=0, microsecond=0).timestamp())
        result: Dict[str, Dict[str, Any]] = {}
        for _, row in df.iterrows():
            try:
                code = str(row[code_col]).strip().zfill(6)
                sym = code_set.get(code)
                if not sym:
                    continue
                last = _safe_float(row.get(last_col))
                if last <= 0:
                    continue
                result[sym] = {
                    "last": last,
                    "name": str(row.get(name_col, "")) if name_col else "",
                    "open": round(_safe_float(row.get(open_col, last)), 4) if open_col else last,
                    "high": round(_safe_float(row.get(high_col, last)), 4) if high_col else last,
                    "low": round(_safe_float(row.get(low_col, last)), 4) if low_col else last,
                    "previousClose": round(_safe_float(row.get(prev_col)), 4) if prev_col else 0,
                    "volume": round(_safe_float(row.get(vol_col)), 2) if vol_col else 0,
                    "symbol": sym, "time": today_ts,
                }
            except (ValueError, TypeError):
                continue
        return result
