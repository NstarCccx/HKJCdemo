import os
import re
import sys
from datetime import datetime, timedelta

from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from database.connection import DB_CONFIG
from database.models import Base
from logger import logger

TABLE = "graphql_jkc_tnc_nodes"

# DEFAULT 分区兜底:未被日分区覆盖的赛日数据落这里,避免插入报错。
# 分区无法用 ORM 模型声明,与日分区一样走动态 DDL
DEFAULT_PARTITION_DDL = """
CREATE TABLE IF NOT EXISTS graphql_jkc_tnc_nodes_default
PARTITION OF graphql_jkc_tnc_nodes DEFAULT
"""


def startup_schema_check(meeting_date):
    """爬虫启动时自检:建库(如缺失)→ 建表 → 确保目标赛日分区存在(幂等)"""
    ensure_database()
    engine = _build_engine()
    try:
        ensure_table(engine)
        ensure_partition(engine, meeting_date)
    finally:
        engine.dispose()


def _build_engine(dbname=None):
    """由 DB_CONFIG 构造 SQLAlchemy engine;CREATE DATABASE 等维护操作传 dbname 覆盖"""
    return create_engine(
        URL.create(
            "postgresql+psycopg2",
            host=DB_CONFIG["host"],
            port=DB_CONFIG["port"],
            database=dbname or DB_CONFIG["dbname"],
            username=DB_CONFIG["user"],
            password=DB_CONFIG["password"],
        ),
        isolation_level="AUTOCOMMIT",  # 建库/建表逐条提交,与旧版 conn.autocommit 一致
    )


def ensure_database():
    """PGDATABASE 指向的库不存在时,通过 postgres 维护库创建(需当前用户有 createdb 权限)"""
    dbname = DB_CONFIG["dbname"]
    if not re.fullmatch(r"[A-Za-z0-9_]+", dbname):
        raise ValueError("库名应仅含字母数字下划线,收到 %s" % dbname)
    engine = _build_engine(dbname="postgres")
    try:
        with engine.connect() as conn:
            exists = conn.execute(
                text("SELECT 1 FROM pg_database WHERE datname = :dbname"), {"dbname": dbname}
            ).fetchone()
            if exists:
                return
            conn.execute(text("CREATE DATABASE %s" % dbname))  # CREATE DATABASE 不能参数化;库名已经正则校验
            logger.info("[INIT] 已创建数据库 %s" % dbname)
    finally:
        engine.dispose()


def ensure_table(engine):
    """按 ORM 模型建分区父表 + 索引 + 唯一约束 + race_horse_mapping(checkfirst 幂等),
    再补 DEFAULT 分区(无声明式表达,走动态 DDL)"""
    Base.metadata.create_all(engine)
    with engine.connect() as conn:
        conn.execute(text(DEFAULT_PARTITION_DDL))
    logger.info("[INIT] 表 %s 结构自检完成" % TABLE)
    logger.info("[INIT] 表 race_horse_mapping 结构自检完成")

def ensure_partition(engine, meeting_date):
    """确保 meeting_date 对应的日分区存在;DEFAULT 分区已兜底,失败不影响启动"""
    if not meeting_date:
        return
    partition = "%s_p%s" % (TABLE, meeting_date.replace("-", ""))
    next_day = (datetime.strptime(meeting_date, "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d")
    sql = "CREATE TABLE IF NOT EXISTS %s PARTITION OF %s FOR VALUES FROM ('%s') TO ('%s')" % (
        partition, TABLE, meeting_date, next_day)
    try:
        with engine.connect() as conn:
            conn.execute(text(sql))
        logger.info("[INIT] 已确保分区 %s (%s ~ %s)" % (partition, meeting_date, next_day))
    except Exception as error:
        # 常见场景:DEFAULT 分区中已有该日期数据,无法收紧分区边界;数据仍会落 DEFAULT,不中断
        logger.warning("创建分区 %s 失败(继续使用 DEFAULT 分区): %s" % (partition, error))