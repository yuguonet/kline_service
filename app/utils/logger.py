# -*- coding: utf-8 -*-
"""日志工具"""
import logging

def get_logger(name: str) -> logging.Logger:
    return logging.getLogger(name)
