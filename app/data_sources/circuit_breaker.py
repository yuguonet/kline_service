# -*- coding: utf-8 -*-
"""
熔断器 — 数据源故障保护
"""

from __future__ import annotations

import threading
import time
from typing import Dict, Optional

from app.utils.logger import get_logger

logger = get_logger(__name__)


class CircuitBreaker:
    """熔断器 — 连续失败超阈值则熔断，冷却后恢复"""

    def __init__(
        self,
        failure_threshold: int = 3,
        cooldown_seconds: float = 300.0,
        name: str = "default",
    ):
        self._failure_threshold = failure_threshold
        self._cooldown_seconds = cooldown_seconds
        self._name = name
        self._failures: Dict[str, int] = {}
        self._tripped_at: Dict[str, float] = {}
        self._lock = threading.Lock()

    def is_available(self, source: str) -> bool:
        """检查源是否可用（未熔断）"""
        with self._lock:
            if source not in self._tripped_at:
                return True
            elapsed = time.time() - self._tripped_at[source]
            if elapsed >= self._cooldown_seconds:
                # 冷却期结束，恢复
                del self._tripped_at[source]
                self._failures[source] = 0
                logger.info(f"[熔断器:{self._name}] {source} 冷却结束，恢复可用")
                return True
            return False

    def record_success(self, source: str):
        """记录成功 — 重置失败计数"""
        with self._lock:
            self._failures[source] = 0
            if source in self._tripped_at:
                del self._tripped_at[source]

    def record_failure(self, source: str, reason: str = ""):
        """记录失败 — 超阈值则熔断"""
        with self._lock:
            self._failures[source] = self._failures.get(source, 0) + 1
            if self._failures[source] >= self._failure_threshold:
                self._tripped_at[source] = time.time()
                logger.warning(
                    f"[熔断器:{self._name}] {source} 连续失败 {self._failures[source]} 次，"
                    f"熔断 {self._cooldown_seconds}s (原因: {reason})"
                )

    def reset(self, source: str = None):
        """手动重置熔断器"""
        with self._lock:
            if source:
                self._failures.pop(source, None)
                self._tripped_at.pop(source, None)
            else:
                self._failures.clear()
                self._tripped_at.clear()


# ================================================================
# 全局熔断器实例
# ================================================================

_realtime_cb = CircuitBreaker(failure_threshold=3, cooldown_seconds=300, name="realtime")
_overseas_cb = CircuitBreaker(failure_threshold=2, cooldown_seconds=900, name="overseas")


def get_realtime_circuit_breaker() -> CircuitBreaker:
    return _realtime_cb


def get_overseas_circuit_breaker() -> CircuitBreaker:
    return _overseas_cb
