import json
import os
import re
import signal
import sys
import time
import traceback
from datetime import datetime, timedelta, timezone
import requests

from logger import logger, setup_logging
from database import startup_schema_check
# .env 由 database/connection.py 导入时经 load_dotenv 注入 os.environ
from database.connection import DB_CONFIG, get_conn
import horse_mapping_crawler

GRAPHQL_URL = "https://info.cld.hkjc.com/graphql/base/"
PLAY_TYPES = ["JKC", "TNC"]
HK_TZ = timezone(timedelta(hours=8))
DEFAULT_INTERVAL_MS = 60000
# 赔率同步独立节拍(方案B):与 --interval 解耦,马匹赔率按自己的间隔高频抓取,
# JKC/TNC 快照轮询频率不受影响
DEFAULT_ODDS_INTERVAL_MS = 10000
# 主循环心跳:每 tick 检查 JKC/TNC 轮询与赔率同步两个独立计时器是否到期
MAIN_LOOP_TICK_MS = 1000
# 赔率同步连续落空(会议结束被 API 清理/网络故障)后的退避上限,
# 同步成功即恢复设定间隔,避免空转日志刷屏
ODDS_BACKOFF_MAX_MS = 600000
DEFAULT_STOP_STABLE = 3
DEFAULT_MAX_HOURS = 30
# 连续整轮(两个玩法都)失败达到该次数时中止进程,避免网络故障后无限空转
MAX_CONSECUTIVE_FAILURES = 30
GRAPHQL_TIMEOUT_S = 30
GRAPHQL_ATTEMPTS = 2

# GraphQL 查询:包含 fragment 的完整查询
FO_QUERY = """fragment racingFoPoolFragment on RacingFoPool {
  instNo
  poolId
  oddsType
  status
  sellStatus
  otherSelNo
  inplayUpTo
  expStartDateTime
  expStopDateTime
  raceStopSellNo
  raceStopSellStatus
  includeRaces
  excludeRaces
  lastUpdateTime
  selections {
    order
    number
    code
    name_en
    name_ch
    scheduleRides
    remainingRides
    points
    lineId
    combId
    combStatus
    openOdds
    prevOdds
    currentOdds
    results {
      raceNo
      points
      point1st
      point2nd
      point3rd
      dhRmk1st
      dhRmk2nd
      dhRmk3rd
      count1st
      count2nd
      count3rd
      count4th
      numerator4th
      denominator4th
    }
  }
  otherSelections {
    order
    code
    name_en
    name_ch
    scheduleRides
    remainingRides
    points
    results {
      raceNo
      points
      point1st
      point2nd
      point3rd
      dhRmk1st
      dhRmk2nd
      dhRmk3rd
      count1st
      count2nd
      count3rd
      count4th
      numerator4th
      denominator4th
    }
  }
}

query resultMeetings($date: String, $venueCode: String, $foOddsTypes: [OddsType], $foFilter: [String], $resultOddsType: [OddsType]) {
  raceMeetings(date: $date, venueCode: $venueCode) {
    id
    resPools: pmPools(oddsTypes: $resultOddsType) {
      leg {
        number
        races
      }
      status
      oddsType
      name_en
      name_ch
      lastUpdateTime
      dividends(officialOnly: true) {
        winComb
        type
        div
        seq
        status
        guarantee
        partial
        partialUnit
      }
      cWinSelections {
        composite
        name_ch
        name_en
        starters
      }
    }
    foPools(oddsTypes: $foOddsTypes, filters: $foFilter) {
      ...racingFoPoolFragment
    }
  }
}"""

# PostgreSQL UPSERT 语
UPSERT_SQL = """
INSERT INTO %TABLE% (
  meeting_date, venue_code, race_no, play_type, pool_id,
  sampled_at, captured_at, last_update_time, inserted_at,
  request_url, request_payload_json,
  payload_json, odds_payload_json, investment_payload_json
) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (meeting_date, venue_code, race_no, play_type, pool_id, sampled_at)
DO UPDATE SET
  captured_at = EXCLUDED.captured_at,
  last_update_time = EXCLUDED.last_update_time,
  inserted_at = EXCLUDED.inserted_at,
  request_url = EXCLUDED.request_url,
  request_payload_json = EXCLUDED.request_payload_json,
  payload_json = EXCLUDED.payload_json,
  odds_payload_json = EXCLUDED.odds_payload_json,
  investment_payload_json = EXCLUDED.investment_payload_json
"""

# ---------------------------------------------------------------------------
# 主流程
# ---------------------------------------------------------------------------

def main():
    options = parse_args(sys.argv[1:])

    # 日志初始化:控制台 + 文件双输出;缺省写脚本目录 logs/crawl_YYYY-MM-DD.log,每日自动切分
    session_id, log_file = setup_logging(options.log_file, file_prefix="crawl")

    logger.info("[CRAWL] 配置 date=%s venue=%s interval=%dms odds-interval=%dms once=%s stop-stable=%d max-hours=%dh table=%s heartbeat=%s" % (
        options.date or "(自动探测)", options.venue, options.interval_ms, options.odds_interval_ms, options.once,
        options.stop_stable, options.max_hours, options.table,
        ("%dmin" % options.heartbeat_min) if options.heartbeat_min else "关闭"))
    logger.info("[CRAWL] 日志会话 sid=%s 文件=%s" % (session_id, log_file))
    if not options.date:
        options.date = resolve_target_date(options)

    # 启动时自检:自动建库(如缺失)、建表、确保目标赛日分区存在(幂等)。
    # 必须在下面 get_conn 之前执行,否则新服务器上库不存在时连接会先失败
    startup_schema_check(options.date)

    # 马匹映射挂钩①:库中无映射时抓取(排位表→赛果页 4 层降级),失败仅 WARN 不阻断 JKC/TNC 轮询
    horse_mapping_crawler.ensure_horse_mapping(options.date)

    conn = get_conn()  # 全局单例(autocommit),断线后下次取用自动重建
    logger.info("[DB] 已连接 %s:%s/%s" % (DB_CONFIG["host"], DB_CONFIG["port"], DB_CONFIG["dbname"]))

    session = {
        "started_at_ms": now_ms(),
        "poll_count": 0,
        "inserted_rows": 0,
        "fail_streak": 0,
        "shutdown_requested": False,
        # 上次心跳时间:静默期定期打状态汇总,用于确认进程存活
        "last_heartbeat_ms": now_ms(),
        # 每个玩法的运行时状态:
        #   observed      是否已见过彩池(开售后为 True)
        #   signature     上一轮 foPool 的 JSON 串,用于稳定计数
        #   stable_count  连续不变的轮数
        #   missing_count 响应中彩池连续缺失的轮数(停售结算后池可能消失)
        #   exp_stop_dt   expStopDateTime 对应 datetime
        #   status        上一轮彩池状态(SELLINGSTARTED / SELLINGSTOPPED 等)
        #   payout_started 是否已观察到 PAYOUTSTARTED(派彩开始,完成判定门槛)
        "play_state": {},
    }
    install_signal_handlers(session)

    upsert_sql = UPSERT_SQL.replace("%TABLE%", options.table)
    stop_reason = "challenge_stable"

    # 双计时器调度(方案B):主循环按 MAIN_LOOP_TICK_MS 心跳,两个任务各按独立间隔触发
    #   - JKC/TNC 快照轮询: options.interval_ms(默认 60 秒,与原单节拍行为一致)
    #   - 赔率同步:        odds_interval_ms(初始 options.odds_interval_ms,默认 10 秒)
    # 赔率同步连续落空(会议结束被 API 清理/网络故障)时逐次翻倍退避至
    # ODDS_BACKOFF_MAX_MS,同步成功即恢复设定间隔,避免每 10 秒打空转日志
    last_poll_ms = 0  # 0 表示启动后立即先跑一轮
    last_odds_ms = 0
    odds_interval_ms = options.odds_interval_ms

    while True:
        if now_ms() - last_poll_ms >= options.interval_ms:
            run_one_poll_cycle(session, options, conn, upsert_sql)
            last_poll_ms = now_ms()
            session["poll_count"] += 1

        # 马匹映射挂钩②:马匹数据实时同步(独立赔率节拍,拉 GraphQL 增量同步
        # 獨贏赔率/退出馬/騎師變更/後備補位,映射存在才同步)
        if now_ms() - last_odds_ms >= odds_interval_ms:
            synced = horse_mapping_crawler.sync_horse_runners_if_needed(options.date)
            last_odds_ms = now_ms()
            odds_interval_ms = (options.odds_interval_ms if synced
                                else min(odds_interval_ms * 2, ODDS_BACKOFF_MAX_MS))

        if options.once:
            stop_reason = "once_mode"
            break
        if session["shutdown_requested"]:
            stop_reason = "signal"
            break
        if all_plays_complete(session, options):
            log_stop_complete(session, options)
            break
        elapsed_hours = (now_ms() - session["started_at_ms"]) / 3600000
        if elapsed_hours >= options.max_hours:
            stop_reason = "max_hours_reached"
            logger.info("[STOP] 已运行 %.1f 小时,达到 max-hours 上限,退出" % elapsed_hours)
            break
        if session["fail_streak"] >= MAX_CONSECUTIVE_FAILURES:
            conn.close()
            raise RuntimeError("连续 %d 轮 GraphQL 请求全部失败,中止进程" % session["fail_streak"])
        maybe_heartbeat(session, options)
        sleep_interruptible(session, MAIN_LOOP_TICK_MS)

    logger.info("[CRAWL] 会话结束 reason=%s 轮询=%d 轮 写入/更新=%d 行 运行=%.1f 分钟" % (
        stop_reason, session["poll_count"], session["inserted_rows"],
        (now_ms() - session["started_at_ms"]) / 60000))
    conn.close()


# 一轮轮询:JKC、TNC 各发一次 GraphQL 请求并落库
def run_one_poll_cycle(session, options, conn, upsert_sql):
    cycle_now_ms = now_ms()
    any_success = False

    for play_type in PLAY_TYPES:
        variables = {
            "date": options.date,
            "venueCode": options.venue,
            "foOddsTypes": [play_type],
            "foFilter": ["top"],
            "resultOddsType": [],
        }
        try:
            result = post_graphql(variables)
        except Exception as error:
            logger.error("job=%s graphql 请求失败: %s" % (play_type, error), exc_info=error)
            continue
        any_success = True

        meeting = result["meeting"]
        if not meeting:
            logger.warning("job=%s 响应无 raceMeetings(date=%s venue=%s),跳过本轮" % (
                play_type, options.date, options.venue))
            continue

        pools = meeting.get("foPools")
        pools = pools if isinstance(pools, list) else []
        if not pools:
            log_poll_waiting(session, play_type, meeting.get("id"))
            continue

        state = session["play_state"].get(play_type)
        if state is None:
            state = session["play_state"][play_type] = new_play_state()

        for pool in pools:
            try:
                # 落库去重闸门:payload 签名与上次已落库签名相同则跳过写入
                signature = dumps_compact(pool)
                if signature == state["last_inserted_signature"]:
                    continue
                insert_pool_row(conn, upsert_sql, options, play_type, pool, cycle_now_ms, result["request_payload"])
                state["last_inserted_signature"] = signature
                session["inserted_rows"] += 1
                logger.info("[POLL] job=%s 数据变化已落库 lastUpdateTime=%s" % (
                    play_type, pool.get("lastUpdateTime") or "(无)"))
            except Exception as error:
                logger.error("job=%s 落库失败: %s" % (play_type, error), exc_info=error)
        update_play_state(session, options, play_type, pools[0], meeting.get("id"))

    session["fail_streak"] = 0 if any_success else session["fail_streak"] + 1


# 彩池状态推进:稳定计数、缺失计数、完成判定日志
def update_play_state(session, options, play_type, pool, meeting_id):
    state = session["play_state"].get(play_type)
    if state is None:
        state = session["play_state"][play_type] = new_play_state()
    state["meeting_id"] = meeting_id

    if pool is None:
        if state["observed"]:
            state["missing_count"] += 1
            logger.warning("job=%s 彩池已观察到但本轮缺失(连续 %d 轮),可能已结算下架" % (
                play_type, state["missing_count"]))
        return
    state["missing_count"] = 0
    if not state["observed"]:
        state["observed"] = True
        logger.info("[POLL] job=%s 彩池出现 meeting=%s poolId=%s expStop=%s" % (
            play_type, meeting_id, pool.get("poolId"), pool.get("expStopDateTime") or "(无)"))
    state["pool_id"] = pool.get("poolId")
    state["status"] = pool.get("status")
    state["exp_stop_dt"] = parse_pool_timestamp(pool.get("expStopDateTime"))
    # 进入派彩/结算阶段后锁定该标记;后续状态变化或彩池下架均不清除
    if pool.get("status") == "PAYOUTSTARTED":
        state["payout_started"] = True

    signature = dumps_compact(pool)
    if signature == state["signature"]:
        state["stable_count"] += 1
    else:
        state["stable_count"] = 0
        state["signature"] = signature


# 单个彩池落库:sampled_at 按分钟对齐,同分钟内重复轮询覆盖为最新 payload
def insert_pool_row(conn, upsert_sql, options, play_type, pool, cycle_now_ms, request_payload):
    sampled_at = datetime.fromtimestamp(cycle_now_ms // 60000 * 60, tz=timezone.utc)
    captured_at = datetime.now(timezone.utc)
    last_update_time = parse_pool_timestamp(pool.get("lastUpdateTime"))
    payload_text = dumps_compact(pool)
    with conn.cursor() as cur:
        cur.execute(upsert_sql, (
            options.date,
            options.venue,
            0,
            play_type,
            None if pool.get("poolId") is None else str(pool.get("poolId")),
            sampled_at,
            captured_at,
            last_update_time,
            datetime.now(timezone.utc),
            GRAPHQL_URL,
            dumps_compact(request_payload),
            payload_text,
            payload_text,
            payload_text,
        ))


# ---------------------------------------------------------------------------
# 辅助函数(按依赖顺序)
# ---------------------------------------------------------------------------
def post_graphql(variables):
    request_payload = {"operationName": "resultMeetings", "query": FO_QUERY, "variables": variables}
    last_error = None
    for attempt in range(1, GRAPHQL_ATTEMPTS + 1):
        try:
            response = requests.post(
                GRAPHQL_URL,
                headers={
                    "content-type": "application/json; charset=utf-8",
                    "accept": "application/json, text/plain, */*",
                    "origin": "https://bet.hkjc.com",
                    "referer": "https://bet.hkjc.com/racing/",
                    "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36",
                },
                data=dumps_compact(request_payload).encode("utf-8"),
                timeout=GRAPHQL_TIMEOUT_S,
            )
            try:
                payload = response.json()
            except ValueError:
                raise RuntimeError("响应非 JSON(status=%d): %s" % (
                    response.status_code, response.text[:120]))
            if not isinstance(payload, dict):
                payload = {}
            raw_errors = payload.get("errors")
            errors = [item.get("message") or str(item) for item in raw_errors] if isinstance(raw_errors, list) else []
            if response.status_code != 200 or errors:
                raise RuntimeError("status=%d errors=%s" % (
                    response.status_code, " | ".join(errors) or "无"))
            meetings = (payload.get("data") or {}).get("raceMeetings") or []
            meeting = meetings[0] if meetings else None
            return {"meeting": meeting, "request_payload": request_payload}
        except Exception as error:
            last_error = error
            if attempt < GRAPHQL_ATTEMPTS:
                time.sleep(0.25)
    raise last_error


# meeting.id 形如 MTG_20260909_0001,中段 8 位即真实赛日(YYYYMMDD)。
# 非赛日请求会返回下一个赛日的 meeting:预售日启动时探测日期与真实赛日错位,
# 落库 meeting_date 必须以 meeting.id 为准,否则 Web 端按错误日期抓排位表
# 得到空壳页面,race_horse_mapping 零行,页面无马号/马名可显示。
MEETING_ID_RE = re.compile(r"^MTG_(\d{4})(\d{2})(\d{2})_")


def date_from_meeting_id(meeting_id):
    if not meeting_id:
        return None
    match = MEETING_ID_RE.match(str(meeting_id))
    if not match:
        return None
    return "%s-%s-%s" % (match.group(1), match.group(2), match.group(3))


# 未指定 --date 时,从 HK 今天起向后探测 4 天,取首个含 challenge 彩池的赛日;
# 锁定日期以 meeting.id 解析出的真实赛日为准(探测日期本身可能是无赛事的预售日)
def resolve_target_date(options):
    for offset in range(0, 4):
        date = hk_date_string(offset)
        try:
            result = post_graphql({
                "date": date,
                "venueCode": options.venue,
                "foOddsTypes": ["JKC"],
                "foFilter": ["top"],
                "resultOddsType": [],
            })
            meeting = result["meeting"] or {}
            pools = meeting.get("foPools") or []
            if pools:
                real_date = date_from_meeting_id(meeting.get("id"))
                if real_date and real_date != date:
                    logger.info("[CRAWL] 探测 %s 命中彩池,但 meeting=%s 属于真实赛日 %s,锁定真实赛日" % (
                        date, meeting.get("id"), real_date))
                    return real_date
                logger.info("[CRAWL] 探测到 %s 有 challenge 彩池(meeting=%s),锁定该赛日" % (
                    date, meeting.get("id")))
                return date
            logger.info("[CRAWL] 探测 %s:无 challenge 彩池(meeting=%s),继续" % (
                date, meeting.get("id") or "无响应"))
        except Exception as error:
            logger.warning("探测 %s 失败: %s,继续" % (date, error))
    raise RuntimeError("未找到含 JKC/TNC 彩池的赛日(已探测 HK 今天起 4 天,venue=%s)" % options.venue)


def all_plays_complete(session, options):
    states = [session["play_state"].get(play_type) for play_type in PLAY_TYPES]
    # 两个玩法都必须出现过彩池
    if not all(state and state["observed"] for state in states):
        return False
    return all(is_play_complete(state, options) for state in states)


def is_play_complete(state, options):
    # 彩池从响应中连续缺失:视为已结算下架
    if state["missing_count"] >= options.stop_stable:
        return True
    # 必须已观察到 PAYOUTSTARTED(进入派彩/结算)才允许按稳定计数完成;
    # 状态推进到 PAYOUTSTARTED 本身会刷新 payload,停售前的平稳轮不会误触发
    if not state["payout_started"]:
        return False
    return state["stable_count"] >= options.stop_stable


def log_stop_complete(session, options):
    for play_type in PLAY_TYPES:
        state = session["play_state"].get(play_type)
        if not state:
            continue
        logger.info("[STOP] job=%s 完成 poolId=%s status=%s stableCount=%d missingCount=%d" % (
            play_type, state["pool_id"], state["status"] or "(未知)",
            state["stable_count"], state["missing_count"]))


# 周期心跳:静默期(数据不变、无异常)也能从日志确认进程存活与运行进度
def maybe_heartbeat(session, options):
    if not options.heartbeat_min:
        return
    now = now_ms()
    if now - session["last_heartbeat_ms"] < options.heartbeat_min * 60000:
        return
    session["last_heartbeat_ms"] = now
    parts = []
    for play_type in PLAY_TYPES:
        state = session["play_state"].get(play_type)
        if not state or not state["observed"]:
            parts.append("%s=未见彩池" % play_type)
        else:
            parts.append("%s=%s stable=%d missing=%d" % (
                play_type, state["status"] or "(未知)",
                state["stable_count"], state["missing_count"]))
    logger.info("[BEAT] 轮次=%d 写入=%d 连续失败=%d %s" % (
        session["poll_count"], session["inserted_rows"],
        session["fail_streak"], " | ".join(parts)))


def log_poll_waiting(session, play_type, meeting_id):
    state = session["play_state"].get(play_type)
    if state is None:
        state = session["play_state"][play_type] = new_play_state()
    if not state["observed"]:
        logger.info("[POLL] job=%s 等待彩池开售(waiting_pool_open, meeting=%s)" % (
            play_type, meeting_id or "无"))
    else:
        state["missing_count"] += 1
        logger.warning("job=%s 彩池已观察到但本轮缺失(连续 %d 轮)" % (
            play_type, state["missing_count"]))


# 解析彩池时间字段:ISO 字符串(如 2026-09-05T17:29:35.557+08:00),非法/缺失返回 None
def parse_pool_timestamp(value):
    if value is None or value == "":
        return None
    try:
        parsed = datetime.fromisoformat(str(value))
    except ValueError:
        return None
    # 无时区的按 HK 时间解释(Node Date.parse 对无时区字符串按本地时区,本机即 UTC+8)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=HK_TZ)
    return parsed


def install_signal_handlers(session):
    def handler(signum, frame):
        logger.info("[CRAWL] 收到 %s,本轮结束后退出" % signal.Signals(signum).name)
        session["shutdown_requested"] = True

    signal.signal(signal.SIGINT, handler)
    signal.signal(signal.SIGTERM, handler)


class _Options(object):
    def __init__(self):
        self.date = ""
        self.venue = "ST"
        self.interval_ms = DEFAULT_INTERVAL_MS
        self.odds_interval_ms = DEFAULT_ODDS_INTERVAL_MS
        self.once = False
        self.stop_stable = DEFAULT_STOP_STABLE
        self.max_hours = DEFAULT_MAX_HOURS
        self.table = "graphql_jkc_tnc_nodes"
        self.log_file = None
        self.heartbeat_min = 10


def parse_args(argv):
    options = _Options()
    usage_text = "\n".join([
        "用法: python3 jkc_tnc_crawler.py [--date YYYY-MM-DD] [--venue ST|HV|S1|S2]",
        "      [--interval 毫秒] [--odds-interval 毫秒] [--once] [--stop-stable N] [--max-hours H] [--table 表名]",
        "      [--log-file 路径] [--heartbeat 分钟]",
        "",
        "  --date         目标赛日(HK 日期)。缺省时自动探测今天起 4 天内首个含 JKC/TNC 的赛日",
        "  --venue        场地代码,与落库 venue_code 一致(ST/HV/S1/S2),默认 ST",
        "  --interval     轮询间隔毫秒,默认 60000(与原系统 challengeActivePollIntervalMs 一致)",
        "  --odds-interval 赔率同步间隔毫秒,默认 10000(与 --interval 解耦:马匹赔率独立节拍,JKC/TNC 轮询频率不变)",
        "  --once         单次快照模式:轮询一轮后退出",
        "  --stop-stable  PAYOUTSTARTED 后 payload 连续不变(或彩池连续缺失)的判定轮数,默认 3",
        "  --max-hours    最长运行小时数(安全上限),默认 30",
        "  --table        写入表名,默认 graphql_jkc_tnc_nodes",
        "  --log-file     日志文件路径,缺省写脚本目录 logs/crawl_YYYY-MM-DD.log 并每日自动切分(控制台同时输出)",
        "  --heartbeat    心跳间隔分钟:静默期定期打一行状态汇总,默认 10,0 关闭",
        "",
        "数据库连接:环境变量 PGHOST/PGPORT/PGDATABASE/PGUSER/PGPASSWORD,默认 127.0.0.1:5432/horse/chen",
    ])

    i = 0
    while i < len(argv):
        arg = argv[i]

        def next_value():
            # 读取当前参数的值;缺失则打印用法并退出
            nonlocal i
            i += 1
            if i >= len(argv):
                sys.stderr.write("错误: 参数 %s 缺少值\n%s\n" % (arg, usage_text))
                sys.exit(1)
            return argv[i]

        if arg == "--date":
            options.date = next_value()
        elif arg == "--venue":
            options.venue = next_value().upper()
        elif arg == "--interval":
            options.interval_ms = to_number(next_value())
        elif arg == "--odds-interval":
            options.odds_interval_ms = to_number(next_value())
        elif arg == "--once":
            options.once = True
        elif arg == "--stop-stable":
            options.stop_stable = to_number(next_value())
        elif arg == "--max-hours":
            options.max_hours = to_number(next_value())
        elif arg == "--table":
            options.table = next_value()
        elif arg == "--log-file":
            options.log_file = next_value()
        elif arg == "--heartbeat":
            options.heartbeat_min = to_number(next_value())
        elif arg == "--help" or arg == "-h":
            print(usage_text)
            sys.exit(0)
        else:
            sys.stderr.write("错误: 未知参数 %s\n%s\n" % (arg, usage_text))
            sys.exit(1)
        i += 1

    if options.date != "" and not re.fullmatch(r"\d{4}-\d{2}-\d{2}", options.date):
        sys.stderr.write("错误: --date 格式应为 YYYY-MM-DD,收到 %s\n" % options.date)
        sys.exit(1)
    if options.interval_ms is None or options.interval_ms < 1000:
        sys.stderr.write("错误: --interval 应为不小于 1000 的毫秒数\n")
        sys.exit(1)
    if options.odds_interval_ms is None or options.odds_interval_ms < 1000:
        sys.stderr.write("错误: --odds-interval 应为不小于 1000 的毫秒数\n")
        sys.exit(1)
    if options.stop_stable is None or options.stop_stable < 1:
        sys.stderr.write("错误: --stop-stable 应为不小于 1 的整数\n")
        sys.exit(1)
    if options.max_hours is None or options.max_hours <= 0:
        sys.stderr.write("错误: --max-hours 应为正数\n")
        sys.exit(1)
    if not re.fullmatch(r"[A-Za-z0-9_]+", options.table):
        sys.stderr.write("错误: --table 应为合法标识符,收到 %s\n" % options.table)
        sys.exit(1)
    if options.heartbeat_min is None or options.heartbeat_min < 0:
        sys.stderr.write("错误: --heartbeat 应为不小于 0 的分钟数(0 表示关闭)\n")
        sys.exit(1)
    return options


# ---------------------------------------------------------------------------
# 通用工具
# ---------------------------------------------------------------------------


def now_ms():
    return int(time.time() * 1000)


# 可被 SIGINT/SIGTERM 打断的 sleep,保证退出及时
def sleep_interruptible(session, total_ms):
    end_at_ms = now_ms() + total_ms
    while now_ms() < end_at_ms and not session["shutdown_requested"]:
        time.sleep(min(1.0, (end_at_ms - now_ms()) / 1000))


# HK 日期字符串:offset_days 天后的 YYYY-MM-DD(香港无夏令时,UTC+8 直算)
def hk_date_string(offset_days=0):
    return (datetime.now(HK_TZ) + timedelta(days=offset_days)).strftime("%Y-%m-%d")


# 与 Node JSON.stringify 等价的紧凑序列化(无空格、保留非 ASCII、保持键序)
def dumps_compact(obj):
    return json.dumps(obj, ensure_ascii=False, separators=(",", ":"))


def new_play_state():
    return {
        "observed": False, "signature": "", "stable_count": 0, "missing_count": 0,
        "exp_stop_dt": None, "status": "", "pool_id": "", "meeting_id": "",
        # 上次已落库的 payload 签名(落库去重闸门,相同则跳过写入)
        "last_inserted_signature": "",
        # 是否已观察到 PAYOUTSTARTED(派彩开始,完成判定门槛)
        "payout_started": False,
    }


def to_number(raw):
    # 对应 Node 的 Number():非法值返回 None,由参数校验统一报错
    try:
        return float(raw)
    except (TypeError, ValueError):
        return None


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        # 探测阶段(信号处理器安装前)的 Ctrl+C 兜底
        logger.critical("KeyboardInterrupt")
        sys.exit(1)
    except Exception:
        logger.critical(traceback.format_exc())
        sys.exit(1)
