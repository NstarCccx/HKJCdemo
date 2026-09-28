"""马匹映射表（race_horse_mapping）— 映射存储、实时同步（獨贏赔率/退出馬/騎師變更）、映射状态查询

每次操作走 transaction() 短连接(非 autocommit):store_horse_mappings / sync_runners_live 的
写操作依赖同事务失败整体回滚,不能复用主循环的 autocommit 单例连接,
否则中途失败会出现"删了没插回"的中间态。
"""

import re

from database.connection import transaction


# ==================== 映射存储 ====================
def store_horse_mappings(meeting_date, mappings):
    """将马匹映射存入数据库（先清空该日期旧数据，整体替换），返回插入行数"""
    if not mappings:
        return 0

    with transaction() as conn:
        with conn.cursor() as cur:
            # 替换语义：重抓时先清掉该日期旧映射，避免脏数据残留
            # （如赛果页对未来日期返回默认页面造成的同场重复数据）；与插入同事务，失败可整体回滚
            cur.execute("DELETE FROM race_horse_mapping WHERE meeting_date = %s", (meeting_date,))
            inserted = 0
            for m in mappings:
                cur.execute(
                    """
                    INSERT INTO race_horse_mapping (meeting_date, race_no, horse_no, horse_name, jockey_name, priority_mark)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT (meeting_date, race_no, jockey_name, horse_no)
                    DO UPDATE SET horse_name = EXCLUDED.horse_name,
                                  priority_mark = EXCLUDED.priority_mark
                    """,
                    (meeting_date, m["race_no"], m.get("horse_no", ""), m["horse_name"], m["jockey_name"],
                     m.get("priority_mark", "")),
                )
                inserted += 1
            return inserted


# ==================== 实时同步（獨贏赔率/退出馬/騎師變更） ====================
def sync_runners_live(meeting_date, runners_by_race):
    """GraphQL 实时 runners 差量同步到 race_horse_mapping，返回 (赔率更新行数, 新增映射行数, 退出清空行数)

    同步规则（按 (race_no, horse_no) 逐马与库中对比）：
      - 退出馬（scratched）: 已有行的 horse_odds 清空
      - 新出现的马（後備補位）: 插入 騎師版+練馬師版 映射行（priority_mark 空串，
        GraphQL 无優先參賽次序数据）
      - 騎師/練馬師/馬名 与库中不一致: 整马重写（先删后插），保留库中原 priority_mark
        （排位表抓取的标记只在库里有，重写时必须带上）
      - 其余: 仅覆盖 horse_odds（騎師版+練馬師版两条一起；odds 为空串时跳过，
        防止 WIN 池暂未开/已收盘把已有赔率冲掉）

    runners_by_race: {race_no: {horse_no: runner}}（来自 GraphQL fetch_live_runners），
    runner 含 odds/name/jockey/trainer/scratched。定位不到且无法插入的马匹跳过，不报错。
    """
    if not runners_by_race:
        return 0, 0, 0

    with transaction() as conn:
        with conn.cursor() as cur:
            # 一次性载入库中该日全部映射，内存中做差量，避免逐马查询
            cur.execute(
                """
                SELECT race_no, horse_no, jockey_name, horse_name, priority_mark, horse_odds
                FROM race_horse_mapping
                WHERE meeting_date = %s
                """,
                (meeting_date,),
            )
            existing = {}
            for race_no, horse_no, jockey_name, horse_name, priority_mark, horse_odds in cur.fetchall():
                existing.setdefault((race_no, str(horse_no)), {})[jockey_name] = {
                    "horse_name": horse_name,
                    "priority_mark": priority_mark or "",
                    "horse_odds": horse_odds or "",
                }

            updated = inserted = cleared = 0
            for race_no, horses in runners_by_race.items():
                for horse_no, runner in horses.items():
                    rows = existing.get((race_no, horse_no))

                    # ① 退出馬：清空已有赔率
                    if runner["scratched"]:
                        if rows and any(row["horse_odds"] for row in rows.values()):
                            cur.execute(
                                """
                                UPDATE race_horse_mapping
                                SET horse_odds = ''
                                WHERE meeting_date = %s AND race_no = %s AND horse_no = %s
                                """,
                                (meeting_date, race_no, horse_no),
                            )
                            cleared += cur.rowcount
                        continue

                    # 期望的映射行：騎師版 + 練馬師版（与排位表/赛果页解析口径一致，
                    # 騎師/練馬師名必须是 2-4 位中文，名字无效时该马跳过不动库中行）
                    expected = {}
                    if runner["jockey"] and _is_chinese_name(runner["jockey"]):
                        expected[runner["jockey"]] = runner["name"]
                    if (runner["trainer"] and _is_chinese_name(runner["trainer"])
                            and runner["trainer"] not in expected):
                        expected[runner["trainer"]] = runner["name"]
                    if not expected:
                        continue

                    # ② 新马（後備補位等）：整组插入
                    if rows is None:
                        for jockey_name in expected:
                            cur.execute(
                                """
                                INSERT INTO race_horse_mapping
                                  (meeting_date, race_no, horse_no, horse_name, jockey_name, priority_mark, horse_odds)
                                VALUES (%s, %s, %s, %s, %s, '', %s)
                                """,
                                (meeting_date, race_no, horse_no, runner["name"],
                                 jockey_name, runner["odds"]),
                            )
                            inserted += 1
                        continue

                    # ③ 结构漂移（騎師/練馬師/馬名变化）：整马重写，保留 priority_mark
                    struct_same = (set(expected) == set(rows)
                                   and all(rows[j]["horse_name"] == runner["name"] for j in expected))
                    if not struct_same:
                        priority_mark = next(iter(rows.values()))["priority_mark"]
                        cur.execute(
                            """
                            DELETE FROM race_horse_mapping
                            WHERE meeting_date = %s AND race_no = %s AND horse_no = %s
                            """,
                            (meeting_date, race_no, horse_no),
                        )
                        for jockey_name in expected:
                            cur.execute(
                                """
                                INSERT INTO race_horse_mapping
                                  (meeting_date, race_no, horse_no, horse_name, jockey_name, priority_mark, horse_odds)
                                VALUES (%s, %s, %s, %s, %s, %s, %s)
                                """,
                                (meeting_date, race_no, horse_no, runner["name"],
                                 jockey_name, priority_mark, runner["odds"]),
                            )
                            inserted += 1
                        continue

                    # ④ 仅赔率变化：覆盖（odds 为空串不覆盖，防止冲掉已有值）
                    if runner["odds"] and any(rows[j]["horse_odds"] != runner["odds"] for j in expected):
                        cur.execute(
                            """
                            UPDATE race_horse_mapping
                            SET horse_odds = %s
                            WHERE meeting_date = %s AND race_no = %s AND horse_no = %s
                            """,
                            (runner["odds"], meeting_date, race_no, horse_no),
                        )
                        updated += cur.rowcount
            return updated, inserted, cleared


def _is_chinese_name(name):
    """騎師/練馬師名校验：2-4 个连续中文字符（与 horse_mapping_crawler 的解析口径一致）"""
    return bool(re.fullmatch(r"[\u4e00-\u9fff]{2,4}", name))


# ==================== 映射状态查询 ====================
def horse_mapping_stats(meeting_date):
    """返回 (映射总行数, 带獨贏赔率的行数)

    供爬虫挂钩判断:total==0 → 需要抓映射;with_odds==0 → 需要补拉赔率。
    """
    with transaction() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT COUNT(*), COUNT(NULLIF(horse_odds, ''))
                FROM race_horse_mapping
                WHERE meeting_date = %s
                """,
                (meeting_date,),
            )
            row = cur.fetchone()
            return int(row[0] or 0), int(row[1] or 0)
