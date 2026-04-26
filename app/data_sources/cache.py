# -*- coding: utf-8 -*-
"""
缓存层 — 两层存储，按数据类型自动路由

设计:
  热数据（短TTL）  →  内存 dict     →  丢了30s重取
  温数据（长TTL）  →  feather 文件  →  进程重启还在

目录结构:
  cache/
    kline/
      CN/SH600519_1D.feather
      HK/HK00700_1D.feather
    stock_info/
      CN/SZ000001.feather
"""

from __future__ import annotations

import time
import threading
from collections import OrderedDict
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

from app.utils.logger import get_logger
from app.data_sources.normalizer import detect_market

logger = get_logger(__name__)


# ================================================================
# TTL 策略 — 按数据类型配置
# ================================================================

DEFAULT_TTL = {
    # 行情 — 秒级时效，内存
    "quote":            30,
    "ticker":           30,
    "index_quotes":     30,
    "market_snapshot":  60,

    # K线 — 按周期分级
    "kline:1m":         60,
    "kline:5m":         120,
    "kline:15m":        180,
    "kline:30m":        300,
    "kline:1H":         300,
    "kline:1D":         14400,
    "kline:1W":         86400,
    "kline:1M":         86400,

    # 市场数据
    "zt_pool":          60,
    "dt_pool":          60,
    "broken_board":     60,
    "dragon_tiger":     300,
    "hot_rank":         300,
    "fund_flow":        300,

    # 基础信息
    "stock_info":       86400,
    "stock_codes":      86400,
    "sector_list":      86400,
}


def get_ttl(data_type: str, timeframe: str = None) -> int:
    """获取数据类型的 TTL"""
    if timeframe and f"{data_type}:{timeframe}" in DEFAULT_TTL:
        return DEFAULT_TTL[f"{data_type}:{timeframe}"]
    return DEFAULT_TTL.get(data_type, 300)


# ================================================================
# 存储后端分类
# ================================================================

DISK_BASE_TYPES = {"stock_info", "stock_codes", "sector_list"}
KLINE_MEMORY_TIMEFRAMES = {"1m", "5m", "15m", "30m", "1H"}
KLINE_DISK_TIMEFRAMES = {"1D", "1W", "1M"}


def should_use_disk(data_type: str) -> bool:
    base = data_type.split(":")[0]
    if base == "kline":
        timeframe = data_type.split(":")[1] if ":" in data_type else None
        if timeframe in KLINE_DISK_TIMEFRAMES:
            return True
        return False
    return base in DISK_BASE_TYPES


# ================================================================
# 缓存键生成
# ================================================================

def make_key(data_type: str, *parts) -> str:
    return ":".join([data_type] + [str(p) for p in parts])


# ================================================================
# 内存缓存
# ================================================================

class CacheEntry:
    __slots__ = ("data", "ts", "ttl", "key")

    def __init__(self, data: Any, ttl: float, key: str):
        self.data = data
        self.ts = time.time()
        self.ttl = ttl
        self.key = key

    def is_valid(self) -> bool:
        return (time.time() - self.ts) < self.ttl


class MemoryCache:
    def __init__(self, max_size: int = 10000):
        self._store: OrderedDict[str, CacheEntry] = OrderedDict()
        self._lock = threading.Lock()
        self._max_size = max_size
        self._hits = 0
        self._misses = 0

    def get(self, key: str) -> Optional[Any]:
        with self._lock:
            entry = self._store.get(key)
            if entry is None:
                self._misses += 1
                return None
            if not entry.is_valid():
                del self._store[key]
                self._misses += 1
                return None
            self._store.move_to_end(key)
            self._hits += 1
            return entry.data

    def set(self, key: str, data: Any, ttl: float = 300) -> None:
        with self._lock:
            if key in self._store:
                self._store.move_to_end(key)
                self._store[key] = CacheEntry(data, ttl, key)
                return
            while len(self._store) >= self._max_size:
                self._store.popitem(last=False)
            self._store[key] = CacheEntry(data, ttl, key)

    def delete(self, key: str) -> None:
        with self._lock:
            self._store.pop(key, None)

    def clear(self, prefix: str = None) -> int:
        with self._lock:
            if prefix is None:
                count = len(self._store)
                self._store.clear()
                return count
            keys = [k for k in self._store if k.startswith(prefix)]
            for k in keys:
                del self._store[k]
            return len(keys)

    def stats(self) -> Dict[str, Any]:
        total = self._hits + self._misses
        return {
            "size": len(self._store),
            "max_size": self._max_size,
            "hits": self._hits,
            "misses": self._misses,
            "hit_rate": f"{self._hits/total:.1%}" if total else "0%",
        }


# ================================================================
# 磁盘缓存 — feather 文件
# ================================================================

class DiskCache:
    def __init__(self, base_dir: str = "cache", max_age_check_interval: int = 300):
        self._base_dir = Path(base_dir)
        self._base_dir.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        self._hits = 0
        self._misses = 0
        self._cleanup_interval = max_age_check_interval
        self._start_cleanup_thread()

    _MARKET_MAP = {"SH": "CN", "SZ": "CN", "BJ": "CN", "HK": "HK"}

    def _file_path(self, data_type: str, market: str, symbol: str,
                   timeframe: str = None) -> Path:
        market_category = self._MARKET_MAP.get(market, "OTHER")
        market_dir = self._base_dir / data_type / market_category
        market_dir.mkdir(parents=True, exist_ok=True)
        filename = f"{market}{symbol}_{timeframe}.feather" if timeframe else f"{market}{symbol}.feather"
        return market_dir / filename

    def _meta_path(self, feather_path: Path) -> Path:
        return feather_path.with_suffix(".meta")

    def _cleanup_empty_dirs(self, dir_path: Path):
        dir_path = dir_path.resolve()
        base = self._base_dir.resolve()
        while dir_path != base and dir_path.is_dir():
            try:
                if not any(dir_path.iterdir()):
                    dir_path.rmdir()
                else:
                    break
            except OSError:
                break
            dir_path = dir_path.parent

    def get(self, data_type: str, market: str, symbol: str,
            timeframe: str = None) -> Optional[Any]:
        import pandas as pd
        fpath = self._file_path(data_type, market, symbol, timeframe)
        mpath = self._meta_path(fpath)
        if not fpath.exists():
            self._misses += 1
            return None
        if mpath.exists():
            try:
                meta = mpath.read_text().split("|")
                write_ts = float(meta[0])
                ttl = float(meta[1])
                if time.time() - write_ts >= ttl:
                    fpath.unlink(missing_ok=True)
                    mpath.unlink(missing_ok=True)
                    self._misses += 1
                    return None
            except (ValueError, IndexError):
                pass
        try:
            df = pd.read_feather(fpath)
            self._hits += 1
            return df
        except Exception as e:
            logger.warning(f"[DiskCache] 读取失败 {fpath}: {e}")
            self._misses += 1
            return None

    def set(self, data_type: str, market: str, symbol: str,
            data: Any, ttl: float, timeframe: str = None) -> None:
        import pandas as pd
        if data is None:
            return
        if isinstance(data, pd.DataFrame) and data.empty:
            return
        fpath = self._file_path(data_type, market, symbol, timeframe)
        mpath = self._meta_path(fpath)
        with self._lock:
            try:
                tmp_path = fpath.with_suffix(".tmp")
                if isinstance(data, pd.DataFrame):
                    data.to_feather(tmp_path)
                else:
                    pd.DataFrame(data).to_feather(tmp_path)
                tmp_path.replace(fpath)
                mpath.write_text(f"{time.time()}|{ttl}")
            except Exception as e:
                logger.warning(f"[DiskCache] 写入失败 {fpath}: {e}")
                try:
                    fpath.with_suffix(".tmp").unlink(missing_ok=True)
                except Exception:
                    pass

    def delete(self, data_type: str, market: str, symbol: str,
               timeframe: str = None) -> None:
        fpath = self._file_path(data_type, market, symbol, timeframe)
        mpath = self._meta_path(fpath)
        fpath.unlink(missing_ok=True)
        mpath.unlink(missing_ok=True)
        self._cleanup_empty_dirs(fpath.parent)

    def clear(self, data_type: str = None, market: str = None) -> int:
        count = 0
        if data_type:
            target = self._base_dir / data_type
            if market:
                target = target / market
        else:
            target = self._base_dir
        if target.exists():
            for f in target.rglob("*.feather"):
                f.unlink(missing_ok=True)
                meta = f.with_suffix(".meta")
                meta.unlink(missing_ok=True)
                count += 1
        return count

    def cleanup_expired(self) -> int:
        count = 0
        now = time.time()
        cleaned_dirs = set()
        for mpath in self._base_dir.rglob("*.meta"):
            try:
                meta = mpath.read_text().split("|")
                write_ts = float(meta[0])
                ttl = float(meta[1])
                if now - write_ts >= ttl:
                    feather = mpath.with_suffix(".feather")
                    feather.unlink(missing_ok=True)
                    mpath.unlink(missing_ok=True)
                    cleaned_dirs.add(mpath.parent)
                    count += 1
            except (ValueError, IndexError, FileNotFoundError):
                pass
        for d in cleaned_dirs:
            self._cleanup_empty_dirs(d)
        if count > 0:
            logger.info(f"[DiskCache] 清理过期文件 {count} 个")
        return count

    def stats(self) -> Dict[str, Any]:
        total = self._hits + self._misses
        file_count = len(list(self._base_dir.rglob("*.feather")))
        return {
            "files": file_count,
            "hits": self._hits,
            "misses": self._misses,
            "hit_rate": f"{self._hits/total:.1%}" if total else "0%",
        }

    def _start_cleanup_thread(self):
        def _loop():
            while True:
                time.sleep(self._cleanup_interval)
                try:
                    self.cleanup_expired()
                except Exception as e:
                    logger.warning(f"[DiskCache] 清理异常: {e}")
        t = threading.Thread(target=_loop, daemon=True, name="disk-cache-cleanup")
        t.start()


# ================================================================
# 统一缓存入口
# ================================================================

class TieredCache:
    def __init__(self, base_dir: str = "cache"):
        self.memory = MemoryCache()
        self.disk = DiskCache(base_dir=base_dir)

    def get(self, key: str, data_type: str) -> Optional[Any]:
        if should_use_disk(data_type):
            parts = key.split(":")
            if len(parts) >= 2:
                symbol = parts[1]
                if symbol:
                    market, digits = detect_market(symbol)
                    if digits:
                        timeframe = parts[2] if len(parts) > 2 else None
                        return self.disk.get(data_type.split(":")[0], market, digits, timeframe)
        return self.memory.get(key)

    def set(self, key: str, data: Any, ttl: float, data_type: str) -> None:
        if should_use_disk(data_type):
            parts = key.split(":")
            if len(parts) >= 2:
                symbol = parts[1]
                if symbol:
                    market, digits = detect_market(symbol)
                    if digits:
                        timeframe = parts[2] if len(parts) > 2 else None
                        self.disk.set(data_type.split(":")[0], market, digits, data, ttl, timeframe)
                        return
        self.memory.set(key, data, ttl)

    def delete(self, key: str, data_type: str = None) -> None:
        if data_type and should_use_disk(data_type):
            parts = key.split(":")
            if len(parts) >= 2:
                symbol = parts[1]
                if symbol:
                    market, digits = detect_market(symbol)
                    if digits:
                        timeframe = parts[2] if len(parts) > 2 else None
                        self.disk.delete(data_type.split(":")[0], market, digits, timeframe)
                        return
        self.memory.delete(key)

    def clear(self, data_type: str = None, market: str = None) -> int:
        count = 0
        if data_type:
            if should_use_disk(data_type):
                count = self.disk.clear(data_type, market)
            else:
                count = self.memory.clear(data_type)
        else:
            count = self.memory.clear()
            count += self.disk.clear()
        return count

    def stats(self) -> Dict[str, Any]:
        return {"memory": self.memory.stats(), "disk": self.disk.stats()}


_cache = TieredCache()

def get_cache() -> TieredCache:
    return _cache


def cached(data_type: str, ttl: float = None, key_fn: Callable = None):
    def decorator(fn):
        def wrapper(*args, **kwargs):
            if key_fn:
                cache_key = make_key(data_type, *key_fn(*args, **kwargs))
            else:
                cache_key = make_key(data_type, *args[1:], **kwargs)
            cached_data = _cache.get(cache_key, data_type)
            if cached_data is not None:
                return cached_data
            result = fn(*args, **kwargs)
            if result is not None and result != [] and result != {}:
                actual_ttl = ttl if ttl is not None else get_ttl(data_type)
                _cache.set(cache_key, result, actual_ttl, data_type)
            return result
        return wrapper
    return decorator
