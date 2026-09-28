from sqlalchemy import (
    BigInteger,
    Column,
    Date,
    Index,
    Integer,
    PrimaryKeyConstraint,
    Text,
    TIMESTAMP,
    UniqueConstraint,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import declarative_base

Base = declarative_base()


# 赛日赔率采样主表(按 meeting_date 分区):id 为复合主键下的 bigserial,
# 复合主键不能只靠列上的 primary_key=True(会按列声明顺序生成),
# 在 __table_args__ 里显式声明以保持 (meeting_date, id) 的旧库顺序
class GraphqlJkcTncNode(Base):
    __tablename__ = "graphql_jkc_tnc_nodes"

    id = Column(BigInteger, autoincrement=True, nullable=False)
    meeting_date = Column(Date, nullable=False)
    venue_code = Column(Text, nullable=False)
    race_no = Column(Integer, nullable=False)
    play_type = Column(Text, nullable=False)
    pool_id = Column(Text)
    sampled_at = Column(TIMESTAMP(timezone=True), nullable=False)
    captured_at = Column(TIMESTAMP(timezone=True))
    last_update_time = Column(TIMESTAMP(timezone=True))
    inserted_at = Column(TIMESTAMP(timezone=True), nullable=False)
    request_url = Column(Text)
    request_payload_json = Column(JSONB)
    payload_json = Column(JSONB)
    odds_payload_json = Column(JSONB)
    investment_payload_json = Column(JSONB)

    __table_args__ = (
        PrimaryKeyConstraint("meeting_date", "id"),
        # 约束名沿用历史 DDL,保证新旧库结构一致
        UniqueConstraint(
            "meeting_date", "venue_code", "race_no", "play_type", "pool_id", "sampled_at",
            name="graphql_jkc_tnc_nodes_new_meeting_date_venue_code_race_no_p_key",
        ),
        Index(
            "idx_graphql_jkc_tnc_nodes_new_lookup",
            "meeting_date", "venue_code", "race_no", "play_type", "sampled_at", "captured_at",
        ),
        Index("idx_graphql_jkc_tnc_nodes_new_sampled_at", text("sampled_at DESC")),
        {"postgresql_partition_by": "RANGE (meeting_date)"},
    )


# 马匹映射表(不分区):马号/马名/骑师(练马师复用同字段)/優先參賽次序标记/獨贏赔率。
# meeting_date 为 TEXT,与既有库表一致;主键顺序显式声明为旧库的 (…, jockey_name, horse_no)
class RaceHorseMapping(Base):
    __tablename__ = "race_horse_mapping"

    meeting_date = Column(Text, nullable=False)
    race_no = Column(Integer, nullable=False)
    horse_no = Column(Text, nullable=False, server_default=text("''"))
    horse_name = Column(Text, nullable=False, server_default=text("''"))
    jockey_name = Column(Text, nullable=False, server_default=text("''"))
    priority_mark = Column(Text, nullable=False, server_default=text("''"))
    horse_odds = Column(Text, nullable=False, server_default=text("''"))

    __table_args__ = (
        PrimaryKeyConstraint("meeting_date", "race_no", "jockey_name", "horse_no"),
    )
