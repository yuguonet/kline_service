# -*- coding: utf-8 -*-
"""
限流器 — 控制对各数据源的请求频率
"""

from __future__ import annotations

import random
import threading
import time
from functools import wraps
from typing import Any, Callable, Dict, Optional, Tuple, Type

from app.utils.logger import get_logger

logger = get_logger(__name__)


class RateLimiter:
    """通用限流器 — 最小间隔 + 随机抖动"""

    def __init__(
        self,
        min_interval: float = 1.0,
        jitter_min: float = 0.0,
        jitter_max: float = 0.0,
    ):
        self._min_interval = min_interval
        self._jitter_min = jitter_min
        self._jitter_max = jitter_max
        self._last_call = 0.0
        self._lock = threading.Lock()

    def wait(self):
        """等待直到可以发起下一次请求"""
        with self._lock:
            now = time.time()
            elapsed = now - self._last_call
            jitter = random.uniform(self._jitter_min, self._jitter_max) if self._jitter_max > 0 else 0
            wait_time = self._min_interval + jitter - elapsed
            if wait_time > 0:
                time.sleep(wait_time)
            self._last_call = time.time()


# ================================================================
# 各源限流器实例
# ================================================================

_tencent_limiter = RateLimiter(min_interval=0.5, jitter_min=0.1, jitter_max=0.5)
_eastmoney_limiter = RateLimiter(min_interval=0.5, jitter_min=0.1, jitter_max=0.5)


def get_tencent_limiter() -> RateLimiter:
    return _tencent_limiter


def get_eastmoney_limiter() -> RateLimiter:
    return _eastmoney_limiter


# ================================================================
# 通用请求头
# ================================================================

def get_request_headers(referer: str = None) -> Dict[str, str]:
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    }
    if referer:
        headers["Referer"] = referer
    return headers


# ================================================================
# 重试装饰器
# ================================================================

def retry_with_backoff(
    max_attempts: int = 3,
    base_delay: float = 1.0,
    max_delay: float = 10.0,
    exceptions: Tuple[Type[Exception], ...] = (Exception,),
):
    """带指数退避的重试装饰器"""
    def decorator(fn):
        @wraps(fn)
        def wrapper(*args, **kwargs):
            last_exc = None
            for attempt in range(max_attempts):
                try:
                    return fn(*args, **kwargs)
                except exceptions as e:
                    last_exc = e
                    if attempt < max_attempts - 1:
                        delay = min(base_delay * (2 ** attempt), max_delay)
                        delay *= random.uniform(0.8, 1.2)
                        logger.debug(f"[重试] {fn.__name__} 第{attempt+1}次失败，{delay:.1f}s后重试: {e}")
                        time.sleep(delay)
            raise last_exc
        return wrapper
    return decorator
