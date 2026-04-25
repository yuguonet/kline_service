# -*- coding: utf-8 -*-
"""
港股数据源 Provider

降级链（国内优先）:
  日/周线 → 腾讯 fqkline → yfinance → AkShare → Twelve Data
  分钟线  → yfinance → AkShare → Twelve Data

能力: K线(全周期) / 单只行情(腾讯)
熔断保护: 海外源熔断器 (2次失败 / 15min冷却)
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from app.data_sources.normalizer import normalize_hk_code
from app.data_sources.rate_limiter import (
    get_request_headers, get_tencent_limiter,
)
from app.data_sources.provider import register
from app.data_sources.circuit_breaker import get_overseas_circuit_breaker
from app.utils.logger import get_logger

logger = get_logger(__name__)


# ================================================================
# 内部工具 — 腾讯港股K线解析
# ================================================================

def _fetch_tencent_hk_kline(
    code: str, period: str, count: int, adj: str = "qfq", timeout: int = 10,
) -> List[Dict[str, Any]]:
    """
    腾讯港股K线 — fqkline 接口。

    Args:
        code:   hk00700 格式
        period: day / week
        count:  请求条数
        adj:    qfq / hfq
    """
    import requests
    from datetime import datetime

    get_tencent_limiter().wait()

    url = "https://web.ifzq.gtimg.cn/appstock/app/fqkline/get"
    params = {"param": f"{code},{period},,,{int(count)}"}

    resp = requests.get(
        url,
        headers=get_request_headers(referer="https://gu.qq.com/"),
        params=params, timeout=timeout,
    )

    try:
        data = resp.json()
    except Exception:
        return []

    if not isinstance(data, dict) or int(data.get("code", 0)) != 0:
        return []

    root = (data.get("data") or {}).get(code)
    if not isinstance(root, dict):
        return []

    # 查找K线数据: 优先 qfqday / qfqweek，其次 day / week
    rows = None
    for key in ([f"{adj}{period}", period] if adj else [period]):
        arr = root.get(key)
        if isinstance(arr, list) and arr:
            rows = arr
            break
    if rows is None:
        for k, v in root.items():
            if isinstance(v, list) and v and str(k).lower().endswith(period):
                rows = v
                break

    if not isinstance(rows, list):
        return []

    out = []
    for r in rows:
        if not isinstance(r, (list, tuple)) or len(r) < 6:
            continue
        try:
            dt_str = str(r[0]).strip()
            ts = None
            for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d", "%Y/%m/%d"):
                try:
                    ts = int(datetime.strptime(dt_str, fmt).timestamp())
                    break
                except ValueError:
                    continue
            if ts is None:
                continue
            o, c, h, low, vol = float(r[1]), float(r[2]), float(r[3]), float(r[4]), float(r[5])
            out.append({
                "time": ts, "open": round(o, 4), "high": round(h, 4),
                "low": round(low, 4), "close": round(c, 4), "volume": round(vol, 2),
            })
        except (ValueError, TypeError, IndexError):
            continue

    out.sort(key=lambda x: x["time"])
    return out[-count:] if len(out) > count else out


def _fetch_tencent_hk_quote(code: str, timeout: int = 8) -> Optional[Dict[str, Any]]:
    """腾讯港股实时行情"""
    import requests

    get_tencent_limiter().wait()
    resp = requests.get(
        f"https://qt.gtimg.cn/q={code}",
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


# ================================================================
# Provider
# ================================================================

@register(priority=40)
class HKStockDataSource:
    """港股数据源 — 腾讯直连 + 海外源降级"""

    name = "hk_stock"
    priority = 40

    capabilities = {
        "kline": True,
        "kline_tf": {"1m", "5m", "15m", "30m", "1H", "1D", "1W"},
        "quote": True,
        "batch_quote": False,  # 港股暂不支持批量行情
        "hk": True,
        "markets": {"HKStock"},
    }

    def __init__(self):
        self.cb = get_overseas_circuit_breaker()

    # ── 行情 ──────────────────────────────────────────────────────

    def fetch_quote(self, code: str, timeout: int = 8) -> Optional[Dict[str, Any]]:
        """港股实时行情 — 腾讯接口"""
        hk_code = normalize_hk_code(code)
        if not hk_code:
            return None
        return _fetch_tencent_hk_quote(hk_code, timeout)

    # ── K线 ──────────────────────────────────────────────────────

    def fetch_kline(
        self, code: str, timeframe: str = "1D", count: int = 300,
        adj: str = "qfq", timeout: int = 10,
    ) -> List[Dict[str, Any]]:
        """
        港股K线 — 多源降级。

        日/周线: 腾讯 → yfinance → AkShare → Twelve Data
        分钟线:  yfinance → AkShare → Twelve Data
        """
        hk_code = normalize_hk_code(code)
        if not hk_code:
            return []

        lim = max(int(count or 300), 1)

        # Tier 1: 腾讯（日/周线，国内直连）
        if timeframe in ("1D", "1W"):
            tf_map = {"1D": "day", "1W": "week"}
            period = tf_map.get(timeframe, "day")
            bars = _fetch_tencent_hk_kline(hk_code, period, lim, adj, timeout)
            if bars:
                self.cb.record_success(self.name)
                return bars

        # Tier 2: yfinance（全周期）
        bars = self._try_yfinance(hk_code, timeframe, lim, timeout)
        if bars:
            self.cb.record_success(self.name)
            return bars

        # Tier 3: AkShare（国内兜底）
        bars = self._try_akshare(hk_code, timeframe, lim, timeout)
        if bars:
            self.cb.record_success(self.name)
            return bars

        # Tier 4: Twelve Data（海外付费，最后降级）
        bars = self._try_twelvedata(hk_code, timeframe, lim, timeout)
        if bars:
            self.cb.record_success(self.name)
            return bars

        return []

    def fetch_quotes_batch(self, codes: List[str], timeout: int = 10) -> Dict[str, Dict[str, Any]]:
        """港股暂不支持批量行情，逐只调用 fetch_quote"""
        result = {}
        for code in codes:
            q = self.fetch_quote(code, timeout)
            if q:
                result[code] = q
        return result

    # ── 海外源降级 ──────────────────────────────────────────────

    def _try_yfinance(
        self, hk_code: str, timeframe: str, limit: int, timeout: int,
    ) -> List[Dict[str, Any]]:
        """yfinance 降级 — 延迟 import，失败不报错"""
        if not self.cb.is_available("yfinance"):
            return []
        try:
            from app.data_sources.asia_stock_kline import fetch_yfinance_klines
            rows = fetch_yfinance_klines(
                is_hk=True, tencent_code=hk_code,
                timeframe=timeframe, limit=limit,
            )
            if rows:
                return rows
        except ImportError:
            logger.debug("[港股] yfinance 不可用，跳过")
        except Exception as e:
            self.cb.record_failure("yfinance", str(e))
        return []

    def _try_akshare(
        self, hk_code: str, timeframe: str, limit: int, timeout: int,
    ) -> List[Dict[str, Any]]:
        """AkShare 降级 — 延迟 import，失败不报错"""
        if not self.cb.is_available("akshare"):
            return []
        try:
            from app.data_sources.asia_stock_kline import (
                fetch_akshare_minute_klines, fetch_akshare_weekly_klines,
            )
            if timeframe in ("1m", "5m", "15m", "30m", "1H", "4H"):
                rows = fetch_akshare_minute_klines(
                    is_hk=True, tencent_code=hk_code,
                    timeframe=timeframe, limit=limit,
                )
            elif timeframe == "1W":
                rows = fetch_akshare_weekly_klines(
                    is_hk=True, tencent_code=hk_code, limit=limit,
                )
            else:
                rows = []
            if rows:
                return rows
        except ImportError:
            logger.debug("[港股] AkShare 不可用，跳过")
        except Exception as e:
            self.cb.record_failure("akshare", str(e))
        return []

    def _try_twelvedata(
        self, hk_code: str, timeframe: str, limit: int, timeout: int,
    ) -> List[Dict[str, Any]]:
        """Twelve Data 降级 — 延迟 import，失败不报错"""
        if not self.cb.is_available("twelvedata"):
            return []
        try:
            from app.data_sources.asia_stock_kline import fetch_twelvedata_klines
            rows = fetch_twelvedata_klines(
                is_hk=True, tencent_code=hk_code,
                timeframe=timeframe, limit=limit,
            )
            if rows:
                return rows
        except ImportError:
            logger.debug("[港股] Twelve Data 不可用，跳过")
        except Exception as e:
            self.cb.record_failure("twelvedata", str(e))
        return []
