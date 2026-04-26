# -*- coding: utf-8 -*-
"""
数据源配置 — 每个源的最大并发数 + 支持的市场 + 吞吐跟踪

设计原则:
- 每个源声明自己支持哪些市场（一个源可能支持多个市场）
- 每个源声明最大并发线程数（由协助层据此调度）
- 协助层据此按源分组，最大化利用每个源的并发能力
- 能批量的绝对不并发（同一批请求尽量合并为一次调用）
- 跟踪每个源的实际吞吐，动态调整分配权重

用法:
    from app.data_sources.source_config import get_source_config, SOURCE_CONFIGS
    cfg = get_source_config("tencent")
    print(cfg.max_workers)   # 5
    print(cfg.markets)       # {"CNStock", "HKStock"}
    print(cfg.throughput)    # 实际 QPS（由 Coordinator 回写）
"""

from __future__ import annotations

import threading
import time
from collections import deque
from dataclasses import dataclass, field
from typing import Dict, Set, Tuple


@dataclass
class SourceConfig:
    """单个数据源的并发/市场/吞吐配置"""

    name: str                          # 源名称（与 Provider.name 对应）
    max_workers: int = 3               # 该源最大并发线程数
    markets: Set[str] = field(default_factory=set)   # 支持的市场集合
    batch_capable: bool = True         # 是否支持批量请求（fetch_quotes_batch）
    batch_size: int = 500              # 单次批量请求的最大条数
    enabled: bool = True               # 是否启用该源

    # ── 吞吐跟踪（真正的滑动窗口，由 Coordinator 回写）──
    _lock: threading.Lock = field(default_factory=threading.Lock, repr=False)
    # 每条记录: (timestamp, success: bool, elapsed: float)
    _window: deque = field(default_factory=lambda: deque(maxlen=10000), repr=False)
    _total_requests: int = 0
    _total_success: int = 0
    _total_time: float = 0.0

    WINDOW_SECONDS: float = 60.0       # 滑动窗口大小（秒）

    def record(self, success: bool, elapsed: float):
        """记录一次请求的结果（由 Coordinator 调用）"""
        now = time.time()
        with self._lock:
            self._window.append((now, success, elapsed))
            self._total_requests += 1
            self._total_time += elapsed
            if success:
                self._total_success += 1

    def _prune_and_calc(self) -> Tuple[int, int, float]:
        """修剪过期条目，返回 (requests, success, total_elapsed) 在窗口内"""
        cutoff = time.time() - self.WINDOW_SECONDS
        while self._window and self._window[0][0] < cutoff:
            self._window.popleft()
        requests = len(self._window)
        success = sum(1 for _, s, _ in self._window if s)
        total_elapsed = sum(e for _, _, e in self._window)
        return requests, success, total_elapsed

    @property
    def throughput(self) -> float:
        """最近窗口的实际 QPS（请求/秒）"""
        with self._lock:
            reqs, _, elapsed = self._prune_and_calc()
            if reqs == 0 or elapsed <= 0:
                return 0.0
            return reqs / elapsed

    @property
    def success_rate(self) -> float:
        """最近窗口的成功率"""
        with self._lock:
            reqs, success, _ = self._prune_and_calc()
            if reqs == 0:
                return 1.0
            return success / reqs

    @property
    def avg_latency(self) -> float:
        """最近窗口的平均延迟（秒）"""
        with self._lock:
            _, success, elapsed = self._prune_and_calc()
            if success == 0:
                return 0.0
            return elapsed / success

    def effective_weight(self) -> float:
        """
        有效权重 = throughput × success_rate。

        用于 Coordinator 动态分配任务：权重高的源多干活。
        没有历史数据时返回 max_workers 作为默认权重。
        """
        t = self.throughput
        if t <= 0:
            return float(self.max_workers)
        return t * self.success_rate

    def stats_summary(self) -> str:
        """返回简短的统计摘要"""
        return (
            f"{self.name}: qps={self.throughput:.1f} "
            f"ok={self.success_rate:.0%} "
            f"lat={self.avg_latency:.2f}s "
            f"workers={self.max_workers}"
        )


# ================================================================
# 源配置注册表
# ================================================================

SOURCE_CONFIGS: Dict[str, SourceConfig] = {

    "tencent": SourceConfig(
        name="tencent",
        max_workers=5,
        markets={"CNStock", "HKStock"},
        batch_capable=True,
        batch_size=500,
    ),

    "sina": SourceConfig(
        name="sina",
        max_workers=3,
        markets={"CNStock"},
        batch_capable=True,
        batch_size=500,
    ),

    "eastmoney": SourceConfig(
        name="eastmoney",
        max_workers=4,
        markets={"CNStock"},
        batch_capable=True,
        batch_size=6000,
    ),

    "akshare": SourceConfig(
        name="akshare",
        max_workers=2,
        markets={"CNStock", "HKStock"},
        batch_capable=True,
        batch_size=5000,
    ),

    "hk_stock": SourceConfig(
        name="hk_stock",
        max_workers=3,
        markets={"HKStock"},
        batch_capable=False,
        batch_size=1,
    ),
}


def get_source_config(name: str) -> SourceConfig:
    """按名称获取源配置，不存在则返回默认配置"""
    return SOURCE_CONFIGS.get(name, SourceConfig(
        name=name,
        max_workers=2,
        markets=set(),
        batch_capable=False,
    ))


def get_sources_for_market(market: str) -> list[SourceConfig]:
    """获取支持指定市场的所有启用源，按 effective_weight 降序"""
    return sorted(
        [c for c in SOURCE_CONFIGS.values()
         if c.enabled and market in c.markets],
        key=lambda c: c.effective_weight(),
        reverse=True,
    )


def get_all_enabled_sources() -> list[SourceConfig]:
    """获取所有启用的源配置"""
    return [c for c in SOURCE_CONFIGS.values() if c.enabled]
