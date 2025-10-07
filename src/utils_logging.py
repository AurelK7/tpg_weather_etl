#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# src/utils_logging.py
import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path
from time import strftime


def setup_logging(
        name: str = "etl",
        level: str = "INFO",
        to_file: bool = True,
        to_console: bool = True,
        log_dir: str = "logs",
        max_bytes: int = 5_000_000,
        backup_count: int = 3,
) -> logging.Logger:
    """
    Configure a namespaced logger with optional file + console handlers.
    Level can be: DEBUG, INFO, WARNING, ERROR.
    """
    logger = logging.getLogger(name)
    # idempotent: avoid duplicate handlers
    if logger.handlers:
        return logger

    logger.setLevel(getattr(logging, level.upper(), logging.INFO))
    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(name)s.%(funcName)s: %(message)s")

    if to_console:
        sh = logging.StreamHandler()
        sh.setLevel(logger.level)
        sh.setFormatter(fmt)
        logger.addHandler(sh)

    if to_file:
        Path(log_dir).mkdir(parents=True, exist_ok=True)
        filename = Path(log_dir) / f"log_{strftime('%Y-%m-%d_%H-%M-%S')}.log"
        fh = RotatingFileHandler(filename, maxBytes=max_bytes, backupCount=backup_count)
        fh.setLevel(logger.level)
        fh.setFormatter(fmt)
        logger.addHandler(fh)

    # Reduce noise from libraries if needed
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("requests").setLevel(logging.WARNING)

    logger.debug("Logger initialized")
    return logger
