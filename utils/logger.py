#! /usr/bin/env python
# -*- coding: utf-8 -*-

import logging
import sys
import os
from typing import Union

try:
    from core.models import LogLevel
except ImportError:
    class LogLevel(Enum): # type: ignore
        DEBUG = "DEBUG"
        INFO = "INFO"
        WARNING = "WARNING"
        ERROR = "ERROR"
        CRITICAL = "CRITICAL"


def setup_logger(name: str, log_file: str, level: Union[LogLevel, str] = LogLevel.INFO) -> logging.Logger:
    """Function to setup as many loggers as you want"""
    log_dir = os.path.dirname(log_file)
    if log_dir and not os.path.exists(log_dir): # Ensure log_dir is not empty string
        try:
            os.makedirs(log_dir)
        except OSError as e:
            print(f"Error creating log directory {log_dir}: {e}")
            _handler = logging.StreamHandler(sys.stdout)
            _formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            _handler.setFormatter(_formatter)
            _logger = logging.getLogger(name)
            # Convert LogLevel Enum to logging level integer
            log_level_int = getattr(logging, level.value if isinstance(level, LogLevel) else level.upper(), logging.INFO)
            _logger.setLevel(log_level_int)
            if not _logger.hasHandlers():
                 _logger.addHandler(_handler)
            return _logger

    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    # File handler
    file_handler = logging.FileHandler(log_file)
    file_handler.setFormatter(formatter)

    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)

    logger = logging.getLogger(name)

    # Convert LogLevel Enum or string to logging level integer
    log_level_int = getattr(logging, level.value if isinstance(level, LogLevel) else level.upper(), logging.INFO)
    logger.setLevel(log_level_int)

    if logger.hasHandlers():
        logger.handlers.clear()

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    return logger
