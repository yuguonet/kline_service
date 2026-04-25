# -*- coding: utf-8 -*-
"""
新浪财经数据源 Provider

能力: K线(日/分钟, 前/后复权) / 单只行情 / 批量行情
特点: 国内直连、无需API Key、速度较快
复权: 通过东财除权除息数据计算复权因子，应用到原始K线
"""

from __future__ import annotations

import json
import re
from datetime import datetime
from typing import Any, Dict, List, Optional

import requests

from app.data_sources.normalizer import to_sina_code
from app.data_sources.rate_limiter import (
    get_request_headers, retry_with_backoff, RateLimiter,
)
from app.data_sources.provider import register
from app.utils.logger import get_logger

logger = get_logger(__name__)


# ================================================================
# 限流器
# ================================================================

_sina_limiter = RateLimiter(
    min_interval=1.5,
    jitter_min=0.8,
    jitter_max=2.5,
)

_sina_quote_limiter = RateLimiter(
    min_interval=0.8,
    jitter_min=0.3,
    jitter_max=1.2,
)
# ================================================================

_SINA_TF_TO_SCALE = {
    "1m": 1, "5m": 5, "15m": 15, "30m": 30, "1H": 60, "1D": 240,
}


def _parse_sina_quote(text: str) -> Optional[Dict[str, Any]]:
    """
    解析新浪行情响应:
    var hq_str_sh600519="贵州茅台,1750.00,1745.00,...";
    """
    m = re.search(r'"(.+?)"', text)
    if not m:
        return None
    parts = m.group(1).split(",")
    if len(parts) < 32:
        return None
    try:
        name = parts[0].strip()
        if not name:
            return None
        open_p = float(parts[1]) if parts[1] else 0.0
        prev_close = float(parts[2]) if parts[2] else 0.0
        last = float(parts[3]) if parts[3] else 0.0
        high = float(parts[4]) if parts[4] else 0.0
        low = float(parts[5]) if parts[5] else 0.0
        volume = float(parts[8]) if parts[8] else 0.0
        amount = float(parts[9]) if parts[9] else 0.0

        if last == 0 and prev_close == 0 and open_p == 0:
            return None

        return {
            "name": name,
            "open": open_p,
            "prev_close": prev_close,
            "last": last,
            "high": high,
            "low": low,
            "volume": volume,
            "amount": amount,
        }
    except (ValueError, IndexError):
        return None


def _sina_kline_to_dicts(data: list, count: int) -> List[Dict[str, Any]]:
    """解析新浪 JSON API K线响应（日K和分钟K通用）"""
    out: List[Dict[str, Any]] = []
    for item in data:
        try:
            dt_str = str(item.get("day", "")).strip()
            if not dt_str:
                continue
            ts = None
            for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d"):
                try:
                    ts = int(datetime.strptime(dt_str, fmt).timestamp())
                    break
                except ValueError:
                    continue
            if ts is None:
                continue

            o = float(item.get("open", 0))
            h = float(item.get("high", 0))
            low = float(item.get("low", 0))
            c = float(item.get("close", 0))
            v = float(item.get("volume", 0))
            if o == 0 and c == 0:
                continue
            out.append({
                "time": ts,
                "open": round(o, 4),
                "high": round(h, 4),
                "low": round(low, 4),
                "close": round(c, 4),
                "volume": round(v, 2),
            })
        except (ValueError, TypeError, KeyError):
            continue

    out.sort(key=lambda x: x["time"])
    if len(out) > count:
        out = out[-count:]
    return out


def _fetch_sina_kline_hisdata(sc: str, count: int, timeout: int) -> List[Dict[str, Any]]:
    """备用: 从 hisdata/klc_kl.js 获取日K线"""
    url = f"https://finance.sina.com.cn/realstock/company/{sc}/hisdata/klc_kl.js"
    _sina_limiter.wait()
    resp = requests.get(
        url,
        headers=get_request_headers(referer="https://finance.sina.com.cn/"),
        timeout=timeout,
    )
    resp.encoding = "gbk"
    text = resp.text or ""

    pattern = re.compile(
        r"(\d{4}-\d{2}-\d{2}),\s*"
        r"([\d.]+),\s*([\d.]+),\s*([\d.]+),\s*([\d.]+),\s*"
        r"([\d.]+)"
    )

    out: List[Dict[str, Any]] = []
    for m in pattern.finditer(text):
        try:
            dt_str, o, c, h, low, v = m.groups()
            ts = int(datetime.strptime(dt_str, "%Y-%m-%d").timestamp())
            o, c, h, low, v = float(o), float(c), float(h), float(low), float(v)
            if o == 0 and c == 0:
                continue
            out.append({
                "time": ts, "open": round(o, 4), "high": round(h, 4),
                "low": round(low, 4), "close": round(c, 4), "volume": round(v, 2),
            })
        except (ValueError, TypeError):
            continue

    if len(out) > count:
        out = out[-count:]
    out.sort(key=lambda x: x["time"])
    return out


# ================================================================
# Provider
# ================================================================

@register(priority=20)
class SinaDataSource:
    """新浪财经 — A股数据源（国内直连、免费、支持复权）"""

    name = "sina"
    priority = 20

    capabilities = {
        "kline": True,
        "kline_tf": {"1m", "5m", "15m", "30m", "1H", "1D"},
        "quote": True,
        "batch_quote": True,
        "hk": False,
        "markets": {"CNStock"},
    }

    # ── K线 ──────────────────────────────────────────────────────

    @retry_with_backoff(max_attempts=3, base_delay=1.5, max_delay=10.0, exceptions=(
        requests.exceptions.RequestException, ConnectionError, TimeoutError,
    ))
    def fetch_kline(
        self, code: str, timeframe: str = "1D", count: int = 300,
        adj: str = "qfq", timeout: int = 10,
    ) -> List[Dict[str, Any]]:
        """
        新浪K线 — 返回原始(未复权)数据，复权由上层统一处理。

        日K: JSON API / hisdata 备用
        分钟K: JSONP 接口
        """
        sc = to_sina_code(code)
        if not sc:
            return []

        scale = _SINA_TF_TO_SCALE.get(timeframe)
        if scale is None:
            return []

        _sina_limiter.wait()

        if timeframe != "1D":
            return self._fetch_minute_kline(sc, scale, count, timeout)

        return self._fetch_raw_daily_kline(sc, count, timeout)

    def _fetch_raw_daily_kline(
        self, sc: str, count: int, timeout: int,
    ) -> List[Dict[str, Any]]:
        """获取新浪原始日K线（不复权）"""
        url = "https://vip.stock.finance.sina.com.cn/cn/api/json.php/CN_MarketDataService.getKLineData"
        params = {
            "symbol": sc,
            "scale": 240,
            "ma": "no",
            "datalen": min(int(count), 2000),
        }

        resp = requests.get(
            url,
            headers=get_request_headers(referer="https://finance.sina.com.cn/"),
            params=params, timeout=timeout,
        )

        try:
            data = resp.json()
        except Exception:
            data = None

        if isinstance(data, list) and data:
            return _sina_kline_to_dicts(data, count)

        # 备用: hisdata/klc_kl
        return _fetch_sina_kline_hisdata(sc, count, timeout)


    def _fetch_minute_kline(
        self, sc: str, scale: int, count: int, timeout: int,
    ) -> List[Dict[str, Any]]:
        """新浪分钟K线 — quotes.sina.cn JSONP 接口"""
        url = "https://quotes.sina.cn/cn/api/jsonp_v2.php/var/CN_MarketDataService.getKLineData"
        params = {
            "symbol": sc,
            "scale": scale,
            "ma": "no",
            "datalen": min(int(count), 2000),
        }

        resp = requests.get(
            url,
            headers=get_request_headers(referer="https://finance.sina.com.cn/"),
            params=params, timeout=timeout,
        )

        text = (resp.text or "").strip()
        m = re.search(r'\[.*\]', text, re.DOTALL)
        if not m:
            return []

        try:
            data = json.loads(m.group())
        except Exception:
            return []

        return _sina_kline_to_dicts(data, count) if isinstance(data, list) else []

    # ── 行情 ──────────────────────────────────────────────────────

    @retry_with_backoff(max_attempts=3, base_delay=1.5, max_delay=10.0, exceptions=(
        requests.exceptions.RequestException, ConnectionError, TimeoutError,
    ))
    def fetch_quote(self, code: str, timeout: int = 8) -> Optional[Dict[str, Any]]:
        """新浪实时行情 — hq.sinajs.cn"""
        sc = to_sina_code(code)
        if not sc:
            return None

        _sina_quote_limiter.wait()
        resp = requests.get(
            f"https://hq.sinajs.cn/list={sc}",
            headers=get_request_headers(referer="https://finance.sina.com.cn/"),
            timeout=timeout,
        )
        resp.encoding = "gbk"
        quote = _parse_sina_quote(resp.text)
        if not quote:
            return None

        quote["symbol"] = sc
        last = quote["last"]
        prev = quote["prev_close"]
        quote["change"] = round(last - prev, 4) if prev else 0.0
        quote["changePercent"] = round(quote["change"] / prev * 100, 2) if prev else 0.0
        quote["open"] = quote.get("open", last) or last
        quote["previousClose"] = prev
        return quote

    def fetch_quotes_batch(self, codes: List[str], timeout: int = 10) -> Dict[str, Dict[str, Any]]:
        """
        新浪批量行情 — 一个 HTTP 请求获取多只股票。
        """
        if not codes:
            return {}
        sina_codes = [to_sina_code(c) for c in codes if c]
        if not sina_codes:
            return {}

        batch_size = 500
        result: Dict[str, Dict[str, Any]] = {}

        for i in range(0, len(sina_codes), batch_size):
            batch = sina_codes[i:i + batch_size]
            query = ",".join(batch)

            _sina_quote_limiter.wait()
            try:
                resp = requests.get(
                    f"https://hq.sinajs.cn/list={query}",
                    headers=get_request_headers(referer="https://finance.sina.com.cn/"),
                    timeout=timeout,
                )
                resp.encoding = "gbk"
            except Exception as e:
                logger.warning(f"[新浪批量行情] 请求失败: {e}")
                continue

            for line in (resp.text or "").strip().split("\n"):
                line = line.strip().rstrip(";")
                m = re.search(r'hq_str_(\w+)="(.+?)"', line)
                if not m:
                    continue
                code_str = m.group(1)
                data = m.group(2)
                parts = data.split(",")
                if len(parts) < 6:
                    continue
                try:
                    name = parts[0].strip()
                    if not name:
                        continue
                    open_p = float(parts[1]) if parts[1] else 0.0
                    prev_close = float(parts[2]) if parts[2] else 0.0
                    last = float(parts[3]) if parts[3] else 0.0
                    high = float(parts[4]) if parts[4] else 0.0
                    low = float(parts[5]) if parts[5] else 0.0
                    vol = float(parts[8]) if len(parts) > 8 and parts[8] else 0.0
                    if last == 0 and prev_close == 0 and open_p == 0:
                        continue
                    chg = round(last - prev_close, 4) if prev_close else 0.0
                    result[code_str] = {
                        "name": name,
                        "last": last,
                        "change": chg,
                        "changePercent": round(chg / prev_close * 100, 2) if prev_close else 0.0,
                        "open": open_p,
                        "high": high,
                        "low": low,
                        "previousClose": prev_close,
                        "volume": vol,
                        "symbol": code_str,
                    }
                except (ValueError, IndexError):
                    continue

        return result
