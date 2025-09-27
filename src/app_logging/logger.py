import logging
import os
from logging.handlers import RotatingFileHandler
from typing import Optional


def setup_logger(level: str, log_dir: str, log_file: str) -> logging.Logger:
    """初始化日志记录器，输出中文流程日志到控制台与文件。"""
    logger = logging.getLogger("dataDig")
    if logger.handlers:
        return logger

    logger.setLevel(getattr(logging, level.upper(), logging.INFO))

    os.makedirs(log_dir, exist_ok=True)
    log_path = os.path.join(log_dir, log_file)

    fmt = logging.Formatter(
        fmt="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    ch = logging.StreamHandler()
    ch.setLevel(getattr(logging, level.upper(), logging.INFO))
    ch.setFormatter(fmt)

    fh = RotatingFileHandler(log_path, maxBytes=10 * 1024 * 1024, backupCount=3, encoding="utf-8")
    fh.setLevel(getattr(logging, level.upper(), logging.INFO))
    fh.setFormatter(fmt)

    logger.addHandler(ch)
    logger.addHandler(fh)

    logger.info("[日志] 日志系统初始化成功，级别=%s，文件=%s", level, log_path)
    return logger
