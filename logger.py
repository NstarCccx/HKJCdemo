import logging
import os
import random
import sys
from datetime import datetime, timedelta, timezone

HK_TZ = timezone(timedelta(hours=8))

# 级别名压缩,保持与原有短标签风格一致
_SHORT_LEVELS = {"WARNING": "WARN", "CRITICAL": "ERROR"}

logger = logging.getLogger("jkc_tnc_crawler")
logger.setLevel(logging.INFO)
logger.propagate = False

class _HKFormatter(logging.Formatter):
    def format(self, record):
        record.levelname = _SHORT_LEVELS.get(record.levelname, record.levelname)
        return super().format(record)

    def formatTime(self, record, datefmt=None):
        return datetime.now(HK_TZ).strftime(datefmt or "%Y-%m-%d %H:%M:%S")

# 缺省日志目录:本文件所在目录(爬虫主目录)下的 logs/
_DEFAULT_LOG_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "logs")

class _DailyFileHandler(logging.StreamHandler):
    """按 HK 时区自然日切分的文件日志:文件名内嵌日期(前缀_YYYY-MM-DD.log),
    跨天自动写新文件;切分靠换文件名而非重命名,多进程同跑互不干扰"""
    def __init__(self, log_dir, prefix):
        super().__init__()
        self._log_dir = os.path.abspath(log_dir)
        self._prefix = prefix
        self._day = None

    def emit(self, record):
        day = datetime.now(HK_TZ).strftime("%Y-%m-%d")
        if day != self._day:
            # 首次写或跨天:关旧文件,切到当天日期的文件(目录自动创建)
            if self.stream:
                self.stream.close()
                self.stream = None
            os.makedirs(self._log_dir, exist_ok=True)
            self._day = day
            self.stream = open(os.path.join(
                self._log_dir, "%s_%s.log" % (self._prefix, day)), "a", encoding="utf-8")
        super().emit(record)


def setup_logging(log_file=None, log_dir=None, file_prefix="crawl"):
    """初始化日志:控制台 + 文件双输出,返回 (会话 ID, 日志文件路径)
    log_file 缺省时写 logs/ 下每日切分文件(目录自动创建),显式指定则写该固定文件"""
    session_id = "%04x" % random.getrandbits(16)
    logger.handlers.clear()

    formatter = _HKFormatter("[HK %(asctime)s] [%(levelname)s] %(message)s")

    console = logging.StreamHandler(sys.stdout)
    console.setFormatter(formatter)
    logger.addHandler(console)

    if log_file:
        os.makedirs(os.path.dirname(os.path.abspath(log_file)), exist_ok=True)
        file_handler = logging.FileHandler(log_file, encoding="utf-8")
    else:
        log_dir = log_dir or _DEFAULT_LOG_DIR
        file_handler = _DailyFileHandler(log_dir, file_prefix)
        log_file = os.path.join(os.path.abspath(log_dir), "%s_%s.log" % (
            file_prefix, datetime.now(HK_TZ).strftime("%Y-%m-%d")))
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    return session_id, log_file
