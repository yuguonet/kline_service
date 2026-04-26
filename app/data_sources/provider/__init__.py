# -*- coding: utf-8 -*-
"""
A股数据源 Provider 框架

设计原则:
- Provider 自注册: import 即注册，上层不感知具体源
- 能力声明: 上层据此选源，不试了才知道
- 统一接口: fetch_kline / fetch_quote 所有源一样
- 编排层按能力 + 熔断状态选源，不写死顺序

用法:
  # tencent.py 里
  @register
  class TencentDataSource:
      name = "tencent"
      capabilities = { ... }
      def fetch_kline(self, code, timeframe, count, adj='qfq'): ...

  # 编排层
  from app.data_sources.provider import get_providers
  for p in get_providers('kline'):
      bars = p.fetch_kline(code, tf, limit)
      if bars: return bars
"""

from __future__ import annotations

import importlib
import pkgutil
import threading
from typing import Any, Callable, Dict, List, Optional, Protocol, Set, Tuple, runtime_checkable

from app.utils.logger import get_logger

logger = get_logger(__name__)


# ================================================================
# 市场类型常量
# ================================================================
# Provider 在 capabilities["markets"] 中声明支持的市场。
# KlineService 按 market 过滤 Provider，避免跨市场误调用。

# 已实现 Provider:
#   CNStock (A股)  → tencent, sina, eastmoney, akshare
#   HKStock (港股) → tencent, akshare, hk_stock

# 待实现 Provider (仅预留常量，暂不注册):
#   USStock (美股) → yfinance / twelvedata / finnhub
#     capabilities: kline, quote
#   Crypto (加密)  → ccxt (binance/okx/bybit)
#     capabilities: kline, quote, batch_quote
#   Forex (外汇)   → twelvedata / tiingo
#     capabilities: kline, quote
#   Futures (期货) → ccxt / eastmoney
#     capabilities: kline, quote


# ================================================================
# Provider 协议 — 所有源必须实现
# ================================================================

@runtime_checkable
class BaseDataSource(Protocol):
    """A股数据源统一接口"""

    name: str
    priority: int  # 越小越优先，默认 100
    capabilities: Dict[str, Any]

    def fetch_kline(
        self, code: str, timeframe: str, count: int,
        adj: str = "qfq", timeout: int = 10,
    ) -> List[Dict[str, Any]]:
        """统一取K线 — 日/周/分钟共用"""
        ...

    def fetch_kline_batch(
        self, codes: List[str], timeframe: str, count: int,
        adj: str = "qfq", timeout: int = 15,
    ) -> Dict[str, List[Dict[str, Any]]]:
        """
        批量K线（单次HTTP或单次API调用）。

        能一次拉全市场的源（如东财 clist）应实现此方法。
        未实现的源自动退化为逐只调用 fetch_kline。
        """
        ...

    def fetch_quote(self, code: str, timeout: int = 8) -> Optional[Dict[str, Any]]:
        """单只实时行情"""
        ...

    def fetch_quotes_batch(self, codes: List[str], timeout: int = 10) -> Dict[str, Dict[str, Any]]:
        """批量行情（单次HTTP）"""
        ...


# ================================================================
# 注册表
# ================================================================

_registry: Dict[str, BaseDataSource] = {}
_lock = threading.Lock()


def register(cls=None, *, priority: int = 100):
    """
    注册 Provider。两种用法:

    @register
    class TencentDataSource: ...

    @register(priority=10)
    class TencentDataSource: ...
    """
    def _do_register(cls):
        provider = cls()
        if not hasattr(provider, 'priority'):
            provider.priority = priority
        with _lock:
            _registry[provider.name] = provider
        logger.debug(f"[Provider] 注册: {provider.name} (priority={provider.priority})")
        return cls

    if cls is not None:
        return _do_register(cls)
    return _do_register


def get_providers(
    capability: str = None,
    timeframe: str = None,
    market: str = None,
) -> List[BaseDataSource]:
    """
    获取可用 provider 列表，按 priority 排序。

    Args:
        capability: 过滤能力 ('kline' / 'kline_batch' / 'quote' / 'batch_quote')
        timeframe:  过滤K线周期 ('1D' / '5m' / ...)
        market:     过滤市场 ('CNStock' / 'HKStock' / ...)
    """
    with _lock:
        providers = list(_registry.values())

    # 按 priority 排序
    providers.sort(key=lambda p: getattr(p, 'priority', 100))

    # 能力过滤
    if capability:
        providers = [
            p for p in providers
            if p.capabilities.get(capability, False)
        ]

    # 周期过滤
    if timeframe:
        providers = [
            p for p in providers
            if timeframe in p.capabilities.get('kline_tf', set())
        ]

    # 市场过滤
    if market:
        providers = [
            p for p in providers
            if market in p.capabilities.get('markets', set())
        ]

    return providers


def get_providers_with_batch(
    timeframe: str = None,
    market: str = None,
) -> List[Tuple[BaseDataSource, bool]]:
    """
    获取 provider 列表，同时标记每个源是否支持批量K线。

    Returns:
        [(provider, has_batch), ...] 按 priority 排序
    """
    providers = get_providers("kline", timeframe=timeframe, market=market)
    result = []
    for p in providers:
        has_batch = p.capabilities.get("kline_batch", False)
        result.append((p, has_batch))
    return result


def get_provider(name: str) -> Optional[BaseDataSource]:
    """按名称获取 provider"""
    return _registry.get(name)


# ================================================================
# 自动发现 — import 时自动注册 provider/ 目录下所有模块
# ================================================================

def autodiscover():
    """
    扫描 app.data_sources.provider 包下所有模块，触发 @register。

    在 app/__init__.py 或启动时调用一次即可。
    """
    package = importlib.import_module("app.data_sources.provider")
    for importer, modname, ispkg in pkgutil.iter_modules(package.__path__):
        if not modname.startswith("_"):
            try:
                importlib.import_module(f"app.data_sources.provider.{modname}")
            except Exception as e:
                logger.warning(f"[Provider] 加载 {modname} 失败: {e}")
