# -*- coding: utf-8 -*-
"""
调度器 — 向后兼容层

核心逻辑已迁移至 DataSourceFactory (factory.py):
  - sequential_fallback → factory.sequential_fallback
  - race                → factory.race
  - InflightDedup       → factory.InflightDedup

本文件保留向后兼容的导入路径，外部代码无需修改。
"""

from app.data_sources.factory import (  # noqa: F401
    InflightDedup,
    sequential_fallback,
    race,
)
