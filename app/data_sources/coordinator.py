# -*- coding: utf-8 -*-
"""
协助层 (Coordinator) — 数据源层与调度层之间的并发协调

定位:
  调度层(KlineService/Dispatcher) → 协助层(Coordinator) → 数据源层(Providers)

核心职责:
  1. 动态队列: 源干完一个活立刻拿下一个，不闲着
  2. 并发控制: 每个源的并发数不超过其 max_workers 配置
  3. 吞吐跟踪: 记录每个源的实际 QPS，动态调整分配优先级
  4. 批量优先: 支持批量接口的源直接调用 fetch_kline_batch
  5. 失败处理: 每个 symbol 最多被所有可用源各试一次，全部失败则放弃

设计原则:
  - 能批量的绝对不并发（调用 provider.fetch_kline_batch）
  - 不能批量的按源并发（每个源独立线池，互不干扰）
  - 最大化利用每个源的并发能力（源支持多少并发就开多少）
  - 不影响现有调度机制（sequential_fallback / race 保持不变）
  - 动态队列保证负载均衡（谁快谁多干）
"""

from __future__ import annotations

import concurrent.futures
import threading
import time
from typing import Any, Callable, Dict, List, Optional, Set, Tuple, TypeVar

from app.data_sources.source_config import (
    SourceConfig, get_source_config, get_sources_for_market,
)
from app.data_sources.circuit_breaker import CircuitBreaker
from app.utils.logger import get_logger

logger = get_logger(__name__)

T = TypeVar("T")

# 单次请求的超时上限（秒），Coordinator 层兜底，防止 fetch_kline 卡死
PER_TASK_TIMEOUT = 20.0

# 队列空时等待的超时（秒），超时后认为所有工作已完成
QUEUE_DRAIN_TIMEOUT = 3.0


# ================================================================
# 线程安全的阻塞任务队列
# ================================================================

class _WorkQueue:
    """
    阻塞任务队列 — 支持等待新任务。

    - get(): 有任务立刻返回，无任务等待最多 QUEUE_DRAIN_TIMEOUT 秒
    - put_back(sym): 放回队尾并唤醒等待线程
    - drain_done(): 标记所有工作完成，唤醒所有等待线程退出
    """

    def __init__(self, items: List[str]):
        self._items = list(items)
        self._cond = threading.Condition()
        self._done = False       # True → 所有工作已完成，线程应退出
        self._pending = 0        # 正在处理中的 symbol 数量

    def get(self) -> Optional[str]:
        """取下一个任务。队列空时等待，超时或 done 时返回 None。"""
        with self._cond:
            while not self._items:
                if self._done:
                    return None
                # 等待新任务或 done 信号
                notified = self._cond.wait(timeout=QUEUE_DRAIN_TIMEOUT)
                if not notified and not self._items:
                    # 超时且仍无任务 → 认为完成
                    return None
            self._pending += 1
            return self._items.pop(0)

    def get_batch(self, batch_size: int) -> List[str]:
        """批量取任务"""
        with self._cond:
            actual = min(batch_size, len(self._items))
            if actual <= 0:
                return []
            batch = self._items[:actual]
            del self._items[:actual]
            self._pending += len(batch)
            return batch

    def put_back(self, sym: str):
        """放回队尾并唤醒等待线程"""
        with self._cond:
            self._items.append(sym)
            self._pending = max(0, self._pending - 1)
            self._cond.notify()

    def task_done(self):
        """标记一个任务完成（不放回队列）"""
        with self._cond:
            self._pending = max(0, self._pending - 1)
            # 如果队列空且无 pending，通知等待线程可能已全部完成
            if not self._items and self._pending == 0:
                self._cond.notify_all()

    def drain_done(self):
        """标记所有工作完成，唤醒所有等待线程退出"""
        with self._cond:
            self._done = True
            self._cond.notify_all()

    @property
    def is_empty(self) -> bool:
        with self._cond:
            return len(self._items) == 0


# ================================================================
# 协助层主类
# ================================================================

class Coordinator:
    """
    协助层 — 动态队列 + 吞吐反馈 + 批量优先。
    """

    def __init__(self):
        self._lock = threading.Lock()

    # ── K线批量协调 ─────────────────────────────────────────────

    def coordinate_kline(
        self,
        symbols: List[str],
        timeframe: str,
        limit: int,
        providers: List[Any],
        cb: CircuitBreaker,
        market: str = "",
        timeout: float = 15.0,
    ) -> Tuple[Dict[str, List[Dict[str, Any]]], List[str]]:
        """
        协调批量K线获取 — 动态队列模式。

        Returns:
            (results, failed)
        """
        if not symbols or not providers:
            return {}, list(symbols)

        # 1. 构建 provider 映射 + 找可用源
        provider_map = {p.name: p for p in providers}
        source_configs = self._get_available_sources(market, provider_map, cb)
        if not source_configs:
            logger.warning(f"[协助层] 市场 {market} 无可用源")
            return {}, list(symbols)

        # 2. 构建阻塞任务队列 + 结果收集
        wq = _WorkQueue(symbols)
        results: Dict[str, List[Dict[str, Any]]] = {}
        results_lock = threading.Lock()
        failed: List[str] = []
        failed_lock = threading.Lock()

        # per-symbol 失败记录
        symbol_tried: Dict[str, Set[str]] = {}
        symbol_tried_lock = threading.Lock()

        # per-source 连续失败计数（R2: 用锁保护原子性）
        source_consecutive_fails: Dict[str, int] = {}
        fails_lock = threading.Lock()
        MAX_SOURCE_FAILS = 5

        def _get_consecutive_fails(name: str) -> int:
            with fails_lock:
                return source_consecutive_fails.get(name, 0)

        def _inc_consecutive_fails(name: str):
            with fails_lock:
                source_consecutive_fails[name] = source_consecutive_fails.get(name, 0) + 1

        def _reset_consecutive_fails(name: str):
            with fails_lock:
                source_consecutive_fails[name] = 0

        def _mark_success(sym: str, bars: List[Dict[str, Any]], source_name: str):
            with results_lock:
                results[sym] = bars

        def _mark_failed(sym: str, source_name: str):
            """标记 symbol 在某个源上失败"""
            with results_lock:
                if sym in results:
                    return  # 已被其他源成功获取

            with symbol_tried_lock:
                tried = symbol_tried.setdefault(sym, set())
                tried.add(source_name)
                untried = [name for name, _ in source_configs if name not in tried]

            if untried:
                wq.put_back(sym)  # 还有源没试过 → 放回队尾
            else:
                with failed_lock:
                    if sym not in failed:
                        failed.append(sym)
                wq.task_done()  # 彻底失败，不再重试

        def _fetch_with_timeout(provider: Any, sym: str, tf: str, lim: int,
                                timeout_s: float) -> Optional[List[Dict[str, Any]]]:
            """R1: 带超时兜底的 fetch_kline 调用"""
            with concurrent.futures.ThreadPoolExecutor(max_workers=1) as task_pool:
                future = task_pool.submit(provider.fetch_kline, sym, tf, lim)
                try:
                    return future.result(timeout=timeout_s)
                except concurrent.futures.TimeoutError:
                    logger.warning(f"[协助层] {provider.name} 获取 {sym} 超时 ({timeout_s}s)")
                    return None

        def _process_symbol(sym: str, source_name: str, provider: Any,
                            cfg: SourceConfig) -> bool:
            """
            处理单个 symbol。返回是否成功。
            R4: 每个 symbol 独立 record()。
            """
            with results_lock:
                if sym in results:
                    wq.task_done()
                    return True

            with symbol_tried_lock:
                symbol_tried.setdefault(sym, set()).add(source_name)

            start_time = time.time()
            try:
                bars = _fetch_with_timeout(provider, sym, timeframe, limit, PER_TASK_TIMEOUT)
                elapsed = time.time() - start_time

                if bars:
                    cb.record_success(source_name)
                    cfg.record(True, elapsed)
                    _mark_success(sym, bars, source_name)
                    _reset_consecutive_fails(source_name)
                    wq.task_done()
                    return True
                else:
                    cb.record_failure(source_name, "empty")
                    cfg.record(False, elapsed)
                    _inc_consecutive_fails(source_name)
                    _mark_failed(sym, source_name)
                    return False
            except Exception as e:
                elapsed = time.time() - start_time
                cb.record_failure(source_name, str(e))
                cfg.record(False, elapsed)
                logger.debug(f"[协助层] {source_name} 获取 {sym} 失败: {e}")
                _inc_consecutive_fails(source_name)
                _mark_failed(sym, source_name)
                return False

        def _worker(source_name: str, cfg: SourceConfig, provider: Any):
            """单个源的 worker 循环"""
            while True:
                if _get_consecutive_fails(source_name) >= MAX_SOURCE_FAILS:
                    break
                if not cb.is_available(source_name):
                    break

                if cfg.batch_capable:
                    batch = wq.get_batch(cfg.batch_size)
                    if not batch:
                        break
                    for sym in batch:
                        _process_symbol(sym, source_name, provider, cfg)
                else:
                    sym = wq.get()
                    if sym is None:
                        break  # 队列空 + 超时 → 退出
                    _process_symbol(sym, source_name, provider, cfg)

        # 3. 构建线程池
        total_threads = 0
        thread_plan = []
        for name, cfg in source_configs:
            if name not in provider_map:
                continue
            tc = 1 if cfg.batch_capable else min(cfg.max_workers, len(symbols))
            thread_plan.append((name, cfg, provider_map[name], tc))
            total_threads += tc

        with concurrent.futures.ThreadPoolExecutor(
            max_workers=total_threads, thread_name_prefix="coord"
        ) as pool:
            futures = []
            for name, cfg, provider, tc in thread_plan:
                for _ in range(tc):
                    futures.append(pool.submit(_worker, name, cfg, provider))

            # 等待所有 worker 完成
            concurrent.futures.wait(futures, timeout=timeout + 2)

        # 4. 标记完成 + 收集剩余
        wq.drain_done()

        # 5. 队列中未处理的 symbol 标记为失败
        # （所有 worker 都退出了但队列里还有 symbol）
        while True:
            sym = wq.get()
            if sym is None:
                break
            with results_lock:
                if sym in results:
                    continue
            with failed_lock:
                if sym not in failed:
                    failed.append(sym)

        # 6. 打印各源吞吐统计
        stats = " | ".join(cfg.stats_summary() for _, cfg in source_configs)
        logger.info(f"[协助层] 完成: {len(results)}成功 {len(failed)}失败 | {stats}")

        return results, failed

    # ── 内部工具 ─────────────────────────────────────────────────

    def _get_available_sources(
        self,
        market: str,
        provider_map: Dict[str, Any],
        cb: CircuitBreaker,
    ) -> List[Tuple[str, SourceConfig]]:
        """获取支持指定市场且未熔断的源列表，按 effective_weight 降序"""
        if market:
            configs = get_sources_for_market(market)
        else:
            from app.data_sources.source_config import get_all_enabled_sources
            configs = get_all_enabled_sources()

        available = []
        for cfg in configs:
            if cfg.name not in provider_map:
                continue
            if not cb.is_available(cfg.name):
                logger.debug(f"[协助层] 源 {cfg.name} 已熔断，跳过")
                continue
            available.append((cfg.name, cfg))

        return available


# ================================================================
# 全局实例
# ================================================================

_coordinator = Coordinator()


def get_coordinator() -> Coordinator:
    """获取全局协助层实例"""
    return _coordinator
