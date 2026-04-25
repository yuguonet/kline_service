# -*- coding: utf-8 -*-
"""
API 感知调度器 — 省调用 + 保时效

核心策略:
1. 缓存优先 — 有缓存就不发请求
2. 请求去重 — 同一 symbol 正在取时，等结果，不重复发
3. 顺序 fallback — K线逐只取，不race（省API）
4. 竞赛 race   — 行情有批量接口，race代价低
5. 动态队列    — 批量K线分源并行，源干完一个抢下一个，不闲着
"""

from __future__ import annotations

import concurrent.futures
import threading
import time
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
                # 已有更快的源返回了，不算失败，只是慢
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


# ================================================================
# 批量分源 — 动态队列（源干完一个立刻拿下一个，不闲着）
# ================================================================

def dynamic_queue(
    symbols: List[str],
    providers: List[Tuple[str, Callable[[str], Optional[T]]]],
    cb: CircuitBreaker,
    timeout: float = 10.0,
    validate: Callable[[T], bool] = lambda x: x is not None,
) -> Tuple[Dict[str, T], List[str]]:
    """
    动态队列 — 源干完一个活立刻拿下一个，不闲着。

    每个 symbol 最多被所有可用源各试一次，全部失败则放弃，不再弹跳。
    返回 (成功结果, 失败symbol列表)，上层据此决定是否反馈给用户或缓存失败。

    流程:
      队列[1,2,3,4,5] → A抢1, B抢2, C抢3
      B完抢4, C完抢5
      A失败 → 放回队首，换B/C接盘（但只换一次，不无限弹）
    """
    if not symbols or not providers:
        return {}, list(symbols)

    available = [(n, f) for n, f in providers if cb.is_available(n)]
    if not available:
        return {}, list(symbols)

    source_names = [n for n, _ in available]

    # 任务队列 + 结果收集
    queue = list(symbols)
    queue_lock = threading.Lock()
    results: Dict[str, T] = {}
    results_lock = threading.Lock()

    # per-symbol 失败记录: symbol → 已试过的源集合
    symbol_tried: Dict[str, set] = {}
    symbol_tried_lock = threading.Lock()
    failed: List[str] = []
    failed_lock = threading.Lock()

    # 每源连续失败计数（连续失败超阈值就暂停该源）
    source_fail_count: Dict[str, int] = {}
    MAX_SOURCE_FAILS = 3

    def _get_next_task() -> Optional[str]:
        """从队列取下一个任务（线程安全）"""
        with queue_lock:
            if queue:
                return queue.pop(0)
        return None

    def _mark_failed(sym: str):
        """标记 symbol 为彻底失败"""
        with failed_lock:
            if sym not in failed:
                failed.append(sym)

    def _worker(source_name: str, fetcher: Callable[[str], Optional[T]]):
        """单个源的工作循环：取任务 → 执行 → 拿下一个"""
        while True:
            # 检查该源是否连续失败太多
            if source_fail_count.get(source_name, 0) >= MAX_SOURCE_FAILS:
                break

            sym = _get_next_task()
            if sym is None:
                break  # 队列空了

            # 记录该源试过这个 symbol
            with symbol_tried_lock:
                symbol_tried.setdefault(sym, set()).add(source_name)

            try:
                data = fetcher(sym)
                if validate(data):
                    cb.record_success(source_name)
                    source_fail_count[source_name] = 0
                    with results_lock:
                        results[sym] = data
                else:
                    cb.record_failure(source_name, "empty/invalid")
                    source_fail_count[source_name] = source_fail_count.get(source_name, 0) + 1
                    _handle_sym_failure(sym, source_name, queue, queue_lock,
                                             symbol_tried, symbol_tried_lock,
                                             source_names, _mark_failed)
            except Exception as e:
                cb.record_failure(source_name, str(e))
                source_fail_count[source_name] = source_fail_count.get(source_name, 0) + 1
                _handle_sym_failure(sym, source_name, queue, queue_lock,
                                         symbol_tried, symbol_tried_lock,
                                         source_names, _mark_failed)

    # 所有源同时启动，各自从队列抢活
    with concurrent.futures.ThreadPoolExecutor(max_workers=len(available)) as pool:
        futures = [
            pool.submit(_worker, name, fn)
            for name, fn in available
        ]
        concurrent.futures.wait(futures, timeout=timeout + 2)

    return results, failed


def _handle_sym_failure(
    sym: str, source_name: str,
    queue: list, queue_lock: threading.Lock,
    symbol_tried: dict, symbol_tried_lock: threading.Lock,
    source_names: list, mark_failed: Callable,
):
    """
    处理单个 symbol 取失败:
    - 如果还有源没试过 → 放回队首让其他源接盘
    - 如果所有源都试过了 → 标记为彻底失败，不放回
    """
    with symbol_tried_lock:
        tried = symbol_tried.get(sym, set())

    # 还有源没试过 → 放回队首
    untried = [n for n in source_names if n not in tried]
    if untried:
        with queue_lock:
            queue.insert(0, sym)
    else:
        # 所有源都试过了，放弃
        mark_failed(sym)
