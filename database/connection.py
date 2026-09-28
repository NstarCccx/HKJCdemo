import os
from contextlib import contextmanager
from pathlib import Path

import psycopg2
from dotenv import load_dotenv

# 读取爬虫主目录 .env,只补缺不覆盖(环境变量优先),之后统一从 os.environ 取值
load_dotenv(Path(__file__).resolve().parent.parent / ".env")

DB_CONFIG = {
    "host": os.environ.get("PGHOST", "127.0.0.1"),
    "port": int(os.environ.get("PGPORT", "5432")),
    "dbname": os.environ.get("PGDATABASE", "horse"),
    "user": os.environ.get("PGUSER", "chen"),
    "password": os.environ.get("PGPASSWORD") or None,
}

_conn = None


def get_conn():
    """全局单例连接(autocommit);连接已断开时下次取用自动重建"""
    global _conn
    if _conn is None or _conn.closed:
        _conn = psycopg2.connect(**DB_CONFIG)
        _conn.autocommit = True  # 每条 upsert 独立提交,与 Node pg 的逐条自动提交行为一致
    return _conn


@contextmanager
def transaction():
    """短连接事务:成功提交,异常回滚并关闭(供需整体回滚的写操作使用)"""
    conn = psycopg2.connect(**DB_CONFIG)
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
