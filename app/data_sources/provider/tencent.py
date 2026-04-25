# -*- coding: utf-8 -*-
"""
腾讯财经数据源 Provider

能力: K线(全周期) / 单只行情 / 批量行情 / 港股
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Optional

import requests

from app.data_sources.normalizer import (
    to_tencent_code, normalize_hk_code,
)
from app.data_sources.rate_limiter import (
    get_request_headers, retry_with_backoff, get_tencent_limiter,
)
from app.data_sources.provider import register
from app.utils.logger import get_logger

logger = get_logger(__name__)


def _lower(code: str) -> str:
    return (code or "").strip().lower()


# 内部周期 → 腾讯参数
_TF_MAP = {
    "1m": ("mkline", "m1"),   "5m": ("mkline", "m5"),
    "15m": ("mkline", "m15"), "30m": ("mkline", "m30"),
    "1H": ("mkline", "m60"),
    "1D": ("fqkline", "day"), "1W": ("fqkline", "week"),
}


def _parse_time(ds: str) -> Optional[int]:
    raw = str(ds or "").strip()
    if not raw:
        return None
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d", "%Y/%m/%d"):
        try:
            return int(datetime.strptime(raw, fmt).timestamp())
        except ValueError:
            continue
    try:
        ts = int(float(raw))
        return int(ts / 1000) if ts > 10**12 else ts
    except Exception:
        return None


def _rows_to_dicts(rows: list) -> List[Dict[str, Any]]:
    out = []
    for r in rows:
        if not isinstance(r, (list, tuple)) or len(r) < 6:
            continue
        ts = _parse_time(r[0])
        if ts is None:
            continue
        try:
            o, c, h, low, vol = float(r[1]), float(r[2]), float(r[3]), float(r[4]), float(r[5])
        except (TypeError, ValueError):
            continue
        out.append({
            "time": ts, "open": round(o, 4), "high": round(h, 4),
            "low": round(low, 4), "close": round(c, 4), "volume": round(vol, 2),
        })
    return out


@register(priority=10)
class TencentDataSource:
    """腾讯财经 — A股首选数据源"""

    name = "tencent"
    priority = 10

    capabilities = {
        "kline": True,
        "kline_tf": {"1m", "5m", "15m", "30m", "1H", "1D", "1W"},
        "quote": True,
        "batch_quote": True,
        "hk": True,
        "markets": {"CNStock", "HKStock"},
    }

    # ── K线 ──────────────────────────────────────────────────────

    @retry_with_backoff(max_attempts=3, base_delay=1.2, max_delay=8.0, exceptions=(Exception,))
    def fetch_kline(
        self, code: str, timeframe: str = "1D", count: int = 300,
        adj: str = "qfq", timeout: int = 10,
    ) -> List[Dict[str, Any]]:
        c = _lower(code)
        if not c:
            return []

        endpoint, tc_tf = _TF_MAP.get(timeframe, (None, None))
        if not endpoint:
            return []

        limiter = get_tencent_limiter()
        limiter.wait()

        # fqkline 不传复权参数 → 返回原始数据，复权由上层统一处理
        if endpoint == "mkline":
            url = "https://proxy.finance.qq.com/ifzqgtimg/appstock/app/kline/mkline"
            params = {"param": f"{c},{tc_tf},{int(count)}"}
        else:
            url = "https://web.ifzq.gtimg.cn/appstock/app/fqkline/get"
            params = {"param": f"{c},{tc_tf},,,{int(count)}"}

        resp = requests.get(
            url, headers=get_request_headers(referer="https://gu.qq.com/"),
            params=params, timeout=timeout,
        )

        try:
            data = resp.json()
        except Exception:
            return []

        if not isinstance(data, dict) or int(data.get("code", 0)) != 0:
            return []

        root = (data.get("data") or {}).get(c)
        if not isinstance(root, dict):
            return []

        rows = None
        if endpoint == "mkline":
            rows = root.get(tc_tf)
        else:
            # 不传复权参数，API 返回原始数据，key 为 tc_tf (day/week)
            arr = root.get(tc_tf)
            if isinstance(arr, list) and arr:
                rows = arr
            if rows is None:
                for k, v in root.items():
                    if isinstance(v, list) and v and str(k).lower().endswith(tc_tf):
                        rows = v
                        break

        return _rows_to_dicts(rows) if isinstance(rows, list) else []

    # ── 行情 ──────────────────────────────────────────────────────

    @retry_with_backoff(max_attempts=3, base_delay=1.2, max_delay=8.0, exceptions=(Exception,))
    def fetch_quote(self, code: str, timeout: int = 8) -> Optional[Dict[str, Any]]:
        c = _lower(code)
        if not c:
            return None

        get_tencent_limiter().wait()
        resp = requests.get(
            f"https://qt.gtimg.cn/q={c}",
            headers=get_request_headers(referer="https://qt.gtimg.cn/"),
            timeout=timeout,
        )
        try:
            resp.encoding = "gbk"
        except Exception:
            pass

        text = (resp.text or "").strip()
        if not text or "~" not in text:
            return None

        try:
            start = text.index('="') + 2
            end = text.rindex('"')
            parts = text[start:end].split("~")
        except Exception:
            return None

        if len(parts) < 6:
            return None

        def _f(i, d=0.0):
            try:
                return float(parts[i]) if i < len(parts) and parts[i] else d
            except Exception:
                return d

        last, prev = _f(3), _f(4)
        chg = round(last - prev, 4) if prev else 0
        return {
            "last": last, "change": chg,
            "changePercent": round(chg / prev * 100, 2) if prev else 0,
            "high": _f(33, last), "low": _f(34, last),
            "open": _f(5) or last, "previousClose": prev,
            "name": (parts[1] or "").strip(),
            "symbol": (parts[2] or "").strip(),
        }

    def fetch_quotes_batch(self, codes: List[str], timeout: int = 10) -> Dict[str, Dict[str, Any]]:
        if not codes:
            return {}
        lowered = [_lower(c) for c in codes if c]
        if not lowered:
            return {}

        result: Dict[str, Dict[str, Any]] = {}
        batch_size = 500

        for i in range(0, len(lowered), batch_size):
            batch = lowered[i:i + batch_size]
            get_tencent_limiter().wait()
            try:
                resp = requests.get(
                    f"https://qt.gtimg.cn/q={','.join(batch)}",
                    headers=get_request_headers(referer="https://qt.gtimg.cn/"),
                    timeout=timeout,
                )
                resp.encoding = "gbk"
            except Exception:
                continue

            for line in (resp.text or "").strip().split("\n"):
                line = line.strip().rstrip(";")
                if "=" not in line or '""' in line:
                    continue
                try:
                    var_name, data = line.split("=", 1)
                    parts = data.strip('"').split("~")
                    if len(parts) < 6 or not parts[1]:
                        continue
                    for c in batch:
                        if c in var_name:
                            last = float(parts[3]) if parts[3] else 0
                            if last <= 0:
                                break
                            prev = float(parts[4]) if parts[4] else 0
                            chg = round(last - prev, 4) if prev else 0
                            result[c] = {
                                "last": last, "change": chg,
                                "changePercent": round(chg / prev * 100, 2) if prev else 0,
                                "high": float(parts[33]) if len(parts) > 33 and parts[33] else last,
                                "low": float(parts[34]) if len(parts) > 34 and parts[34] else last,
                                "open": float(parts[5]) if parts[5] else last,
                                "previousClose": prev,
                                "name": parts[1].strip(),
                                "symbol": parts[2].strip(),
                            }
                            break
                except Exception:
                    continue

        return result
