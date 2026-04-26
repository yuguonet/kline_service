# -*- coding: utf-8 -*-
"""
股票代码标准化模块 — 统一市场判断 + 各数据源格式转换

核心函数 detect_market() 返回 (market, digits)，各数据源只做格式拼接。
消除各 Provider 中重复的市场判断逻辑。

支持输入:
  600519 / 600519.SH / sh600519 / SZ000001 / 830799.BJ
"""

from typing import Any, Tuple


# ================================================================
# 安全类型转换 — 从旧架构迁移，市场数据解析常用
# ================================================================

def safe_float(v: Any, default: float = 0.0) -> float:
    """安全转 float，"-"/""/None → default"""
    if v is None or v == "-" or v == "":
        return default
    try:
        return float(v)
    except (TypeError, ValueError):
        return default

def safe_int(v: Any, default: int = 0) -> int:
    """安全转 int，"-"/""/None → default"""
    if v is None or v == "-" or v == "":
        return default
    try:
        return int(float(v))
    except (TypeError, ValueError):
        return default

def detect_market(code: str) -> Tuple[str, str]:
    """
    统一市场判断。返回 (market, digits)。

    Args:
        code: 任意格式的股票代码

    Returns:
        (market, digits) — market 为 'SH'/'SZ'/'BJ'/''，digits 为纯6位数字

    Examples:
        detect_market('600519')      → ('SH', '600519')
        detect_market('sh600519')    → ('SH', '600519')
        detect_market('600519.SH')   → ('SH', '600519')
        detect_market('SZ000001')    → ('SZ', '000001')
        detect_market('830799.BJ')   → ('BJ', '830799')
        detect_market('UNKNOWN')     → ('', 'UNKNOWN')
    """
    s = (code or "").strip().upper()
    if not s:
        return "", ""

    # 1) 带后缀: 600519.SH / 600519.SS / 600519.SZ / 830799.BJ
    for suffix in (".SH", ".SS", ".SZ", ".BJ"):
        if s.endswith(suffix):
            return s[-2:], s[:-3]

    # 2) 带前缀: SH600519 / SZ000001 / BJ830799
    if s.startswith(("SH", "SZ", "BJ")) and len(s) >= 3:
        rest = s[2:]
        if rest.isdigit() and len(rest) == 6:
            return s[:2], rest

    # 3) 纯6位数字: 600519 / 000001 / 830799 / 510050
    if s.isdigit() and len(s) == 6:
        # 沪市: 主板60x, 科创板688/689, B股900, ETF51x, 可转债110/113/118
        if s.startswith(("600", "601", "603", "605", "688", "689", "900",
                         "510", "511", "512", "513", "515", "516", "518", "519",
                         "110", "113", "118")):
            return "SH", s
        # 深市: 主板00x, 创业板300/301, B股200, 基金15/16/18, 可转债127/128
        if s.startswith(("000", "001", "002", "003", "300", "301", "200",
                         "150", "159", "160", "161", "162", "163", "164", "165",
                         "166", "167", "168", "169",
                         "180", "184", "185", "186", "187", "188", "189",
                         "127", "128")):
            return "SZ", s
        # 北证: 43/82/83/87/88
        if s.startswith(("43", "82", "83", "87", "88")):
            return "BJ", s

    return "", s


# ================================================================
# 各数据源格式转换
# ================================================================

def to_tencent_code(code: str) -> str:
    """
    转换为腾讯格式: sh600519 / sz000001 / bj830799

    Examples:
        to_tencent_code('600519')    → 'sh600519'
        to_tencent_code('SH600519')  → 'sh600519'
        to_tencent_code('600519.SH') → 'sh600519'
    """
    market, digits = detect_market(code)
    if market and digits:
        return f"{market.lower()}{digits}"
    return (code or "").strip().lower()

def to_sina_code(code: str) -> str:
    """
    转换为新浪格式: sh600519 / sz000001 / bj830799

    新浪格式与腾讯相同（小写前缀 + 数字）。
    """
    return to_tencent_code(code)

def to_eastmoney_secid(code: str) -> str:
    """
    转换为东财 secid 格式: 1.600519 / 0.000001 / 0.830799

    沪市(含科创板) → 1.xxx
    深市(含创业板/北证) → 0.xxx

    Examples:
        to_eastmoney_secid('600519')    → '1.600519'
        to_eastmoney_secid('000001')    → '0.000001'
        to_eastmoney_secid('830799')    → '0.830799'
        to_eastmoney_secid('UNKNOWN')   → ''
    """
    market, digits = detect_market(code)
    if not market or not digits:
        return ""
    if market == "SH":
        return f"1.{digits}"
    # SZ / BJ
    return f"0.{digits}"

def to_raw_digits(code: str) -> str:
    """
    提取纯6位数字代码（剥离所有前缀/后缀）。

    Examples:
        to_raw_digits('SH600519')  → '600519'
        to_raw_digits('000001.SZ') → '000001'
        to_raw_digits('600519')    → '600519'
    """
    _, digits = detect_market(code)
    return digits

def to_canonical(code: str) -> str:
    """
    转换为标准格式: SH600519 / SZ000001 / BJ830799

    Examples:
        to_canonical('600519')    → 'SH600519'
        to_canonical('sh600519')  → 'SH600519'
        to_canonical('600519.SH') → 'SH600519'
    """
    market, digits = detect_market(code)
    if market and digits:
        return f"{market}{digits}"
    return (code or "").strip().upper()


# ================================================================
# 港股代码标准化
# ================================================================

def normalize_hk_code(symbol: str) -> str:
    """
    港股代码标准化: hk00700 (5位数字)。

    支持输入: 700 / 0700 / 00700.HK / HK700

    Examples:
        normalize_hk_code('700')      → 'HK00700'
        normalize_hk_code('00700.HK') → 'HK00700'
        normalize_hk_code('HK700')    → 'HK00700'
    """
    s = (symbol or "").strip().upper()
    if not s:
        return s
    if s.endswith(".HK"):
        s = s[:-3]
    if s.isdigit():
        return "HK" + s.zfill(5)
    if s.startswith("HK") and s[2:].isdigit():
        return "HK" + s[2:].zfill(5)
    return s
