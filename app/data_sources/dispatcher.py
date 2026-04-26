# -*- coding: utf-8 -*-
"""
调度器 — 单源 fallback + 竞赛

核心策略（保持不变）:
1. 缓存优先 — 有缓存就不发请求
2. 请求去重 — 同一 symbol 正在取时，等结果，不重复发
3. 顺序 fallback — K线逐只取，不race（省API）
4. 竞赛 race   — 行情有批量接口，race代价低

变更说明:
  原 dynamic_queue（批量并行）已移至协助层 coordinator.py。
  协助层在数据源层和调度层之间，负责:
    - 按源分组 symbols
    - 每个源的并发控制（max_workers）
    - 批量优先（能批量的绝不并发）
  调度层不再直接管理线程池，只负责单源 fallback 和竞赛。
"""

from __future__ import annotations

import concurrent.futures
import threading
from typing import Any, Callable, Dict, List, Optional, Tuple, TypeVar

from app.data_sources.circuit_breaker import CircuitBreaker
from app.utils.logger import get_logger

logger = get_logger(__name__)

T = TypeVar("T")


# ================================================================
# 请求去重 — 同一 symbol 正在取时，等结果不重复发
# ================================================================

class InflightDedup:
    """
    请求去重器。

    如果 symbol A 正在被某个线程取数据，其他线程对 A 的请求
    直接等结果，不重复发 API 调用。
    """

    def __init__(self, max_workers: int = 4):
        self._lock = threading.Lock()
        self._inflight: Dict[str, concurrent.futures.Future] = {}
        self._executor = concurrent.futures.ThreadPoolExecutor(
            max_workers=max_workers, thread_name_prefix="dedup",
        )

    def get_or_submit(self, key: str, fn: Callable[[], T]) -> T:
        """
        获取已有结果或提交新请求。

        如果 key 已有 inflight 请求，等结果。
        否则提交新请求到内部线程池，其他线程可以等这个结果。
        """
        with self._lock:
            if key in self._inflight:
                future = self._inflight[key]
            else:
                future = self._executor.submit(fn)
                self._inflight[key] = future

        try:
            return future.result(timeout=30)
        finally:
            with self._lock:
                if key in self._inflight and self._inflight[key] is future:
                    del self._inflight[key]


# 全局去重实例
_dedup = InflightDedup()


# ================================================================
# 顺序 Fallback — K线专用（省API）
# ================================================================

def sequential_fallback(
    symbol: str,
    providers: List[Tuple[str, Callable[[], Optional[T]]]],
    cb: CircuitBreaker,
    validate: Callable[[T], bool] = lambda x: x is not None,
) -> Tuple[Optional[T], Optional[str]]:
    """
    顺序 fallback — 一个一个试，成功就停。

    适合 K线（每只1次HTTP，race浪费API）。
    按 priority 顺序尝试，第一个成功的立即返回。

    Args:
        symbol:     股票代码
        providers:  [(name, fetcher), ...]
        cb:         熔断器
        validate:   结果校验

    Returns:
        (result, source_name) 或 (None, None)
    """
    for name, fetcher in providers:
        if not cb.is_available(name):
            continue
        try:
            result = fetcher()
            if validate(result):
                cb.record_success(name)
                return result, name
            cb.record_failure(name, "empty/invalid")
        except Exception as e:
            cb.record_failure(name, str(e))

    return None, None


# ================================================================
# 竞赛 Race — 行情专用（有批量接口，代价低）
# ================================================================

def race(
    providers: List[Tuple[str, Callable[[], Optional[T]]]],
    cb: CircuitBreaker,
    timeout: float = 8.0,
    validate: Callable[[T], bool] = lambda x: x is not None,
) -> Tuple[Optional[T], Optional[str]]:
    """
    并发竞赛 — 多源同时取，第一个有效结果返回。

    适合行情（有批量接口，race代价低）。
    不适合K线（逐只取，race浪费API）。
    """
    available = [(n, f) for n, f in providers if cb.is_available(n)]
    if not available:
        return None, None

    if len(available) == 1:
        name, fn = available[0]
        try:
            result = fn()
            if validate(result):
                cb.record_success(name)
                return result, name
            cb.record_failure(name, "empty/invalid")
        except Exception as e:
            cb.record_failure(name, str(e))
        return None, None

    result_holder: Dict[str, Any] = {"result": None, "source": None}
    done_event = threading.Event()
    lock = threading.Lock()

    def _try(source_name: str, fetcher: Callable) -> None:
        try:
            data = fetcher()
            if done_event.is_set():
                return
            if validate(data):
                with lock:
                    if result_holder["result"] is None:
                        result_holder["result"] = data
                        result_holder["source"] = source_name
                        done_event.set()
                cb.record_success(source_name)
            else:
                cb.record_failure(source_name, "empty/invalid")
        except Exception as e:
            if not done_event.is_set():
                cb.record_failure(source_name, str(e))

    executor = concurrent.futures.ThreadPoolExecutor(max_workers=len(available))
    try:
        futures = {executor.submit(_try, n, f): n for n, f in available}
        done_event.wait(timeout=timeout + 1)
    finally:
        executor.shutdown(wait=False)

    return result_holder["result"], result_holder["source"]
