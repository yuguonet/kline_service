# -*- coding: utf-8 -*-
"""
数据源模块
"""
from app.data_sources.factory import DataSourceFactory, get_factory, SourceAdapter
from app.data_sources.circuit_breaker import (
    CircuitBreaker,
    get_realtime_circuit_breaker,
    get_overseas_circuit_breaker,
)
from app.data_sources.normalizer import detect_market

__all__ = [
    'DataSourceFactory',
    'get_factory',
    'SourceAdapter',
    'CircuitBreaker',
    'get_realtime_circuit_breaker',
    'get_overseas_circuit_breaker',
    'detect_market',
]
