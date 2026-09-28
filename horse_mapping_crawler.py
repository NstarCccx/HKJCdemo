"""HKJC 马匹数据爬虫 — 马名马号映射 + 獨贏赔率
数据来源: 香港赛马会 (HKJC) 排位表(racecard) / 赛果(localresults) 页面,
         以及 bet.hkjc.com「獨贏/位置」页背后的 GraphQL(与 JKC/TNC 快照数据同源)
抓取范围: 多场赛事的 馬號 ↔ 馬名 ↔ 騎師/練馬師 映射关系, 每匹马的獨贏(WIN)赔率
运行方式:
  1. 命令行独立运行: python3 horse_mapping_crawler.py --date YYYY-MM-DD [--refresh-odds]
  2. 由 jkc_tnc_crawler.py 自动挂钩:
     启动阶段 ensure_horse_mapping()(库中无映射时抓取),
     轮询循环 sync_horse_runners_if_needed()(每轮实时同步獨贏赔率/退出馬/騎師變更/後備補位)
"""

import os
import re
import sys
import time
import traceback
from datetime import datetime, timedelta, timezone
from typing import Optional

import requests
from bs4 import BeautifulSoup

from logger import logger, setup_logging
from database.horse_mapping_repo import (
    store_horse_mappings,
    sync_runners_live,
    horse_mapping_stats,
)

HK_TZ = timezone(timedelta(hours=8))

# ==================== 常量 ====================
RESULTS_URL = "https://racing.hkjc.com/racing/information/Chinese/Racing/LocalResults.aspx"
# 排位表新版 URL（旧 RaceCard.aspx 会 302 到此地址；直接使用新版避免多一跳）
RACECARD_URL = "https://racing.hkjc.com/zh-hk/local/information/racecard"

# racing.hkjc.com（新官网）页面背后的 GraphQL（与 JKC/TNC 快照数据同源，匿名可访问）。
# 该服务按 query 文本白名单放行，必须原样重放页面 JS 捕获的完整 query（行尾空白可省略），
# 任何裁剪都会被 WHITELIST_ERROR 拒绝。官方 query 按页面拆分且不可修改合并：
# 騎師座騎页的 rw_JockeysRides 提供 馬名/騎師/狀態，練馬師馬匹页的 rw_TrainersEntries
# 提供 練馬師，各拉一次按 (场次,马号) 合并（缺任一侧会触发同步的结构漂移整马重写、
# 误删另一版映射行）。獨贏赔率在会议级 pmPools(oddsTypes:[WIN])[].oddsNodes
# （leg.number=场次, combString=补零马号"01", oddsValue=赔率, '---'=暂未发布），
# 官方页面即用此数据；runners[].winOdds 在晨早赔率阶段恒为空不可用（bet.hkjc.com
# 旧 raceMeetings query 读该字段曾致赔率长期抓不到）。commonMeetings 按 dates
# 过滤返回（query 带 active:true，会议结束被清理后不再返回），
# 解析时按 date 精确匹配校验，防止把别的赛事日赔率写进库。
ODDS_GRAPHQL_URL = "https://info.cld.hkjc.com/graphql/base/"
JOCKEY_RIDES_GRAPHQL_QUERY = """query rw_JockeysRides ($dates: [String!]) {
  commonMeetings(dates: $dates, venueCodes: ["ST", "HV"], active: true) {
    date
    venueCode
    status
    races {
      no
      claCode
      distance
      status
      raceTrack {
        code
        description_ch
        description_en
      }
      runners {
        horse {
          code
          name_ch
          name_en
          id
        }
        jockey {
          name_en
          name_ch
          code
        }
        id
        no
        winOdds
        status
        trumpCard
      }
    }
    pmPools(oddsTypes: [WIN]) {
      status
      sellStatus
      id
      leg {
        number
        races
      }
      oddsNodes {
        oddsValue
        combString
        hotFavourite
      }
    }
    changeHistories {
      type
      time
      raceNo
      runnerNo
      horseName_ch
      horseName_en
      jockeyName_ch
      jockeyName_en
      scratchHorseName_ch
      scratchHorseName_en
      handicapWeight
      scrResvIndicator
      horseCode
      dueToPromotion
    }
    isActive
  }
  jockeyStat {
    code
    name_en
    name_ch
    ssnStat {
      numStarts
      numFirst
      numSecond
      numThird
      numFourth
      numFifth
      ven
      trk
      dist
      stakeWon
    }
    dhStat {
      numStarts
      numFirst
      numSecond
      numThird
      numFourth
      numFifth
      ven
      trk
      dist
      stakeWon
    }
  }
}"""
TRAINERS_ENTRIES_GRAPHQL_QUERY = """query rw_TrainersEntries ($dates: [String!]) {
  commonMeetings(dates: $dates, venueCodes: ["ST", "HV"], active: true) {
    date
    venueCode
    status
    totalNumberOfRace
    raNoPlCount
    races {
      no
      status
      claCode
      distance
      raceTrack {
        code
        description_ch
        description_en
      }
      runners {
        horse {
          code
          name_ch
          name_en
          id
        }
        trainer {
          name_en
          name_ch
          code
        }
        trainerPreference
        id
        no
        winOdds
        status
        trumpCard
        priority
      }
    }
    pmPools(oddsTypes: [WIN]) {
      status
      sellStatus
      id
      leg {
        number
        races
      }
      oddsNodes {
        oddsValue
        combString
        hotFavourite
      }
    }
    isActive
  }
  trainerStat {
    code
    name_en
    name_ch
    ssnStat {
      numStarts
      numFirst
      numSecond
      numThird
      numFourth
      numFifth
      ven
      trk
      dist
      stakeWon
    }
    dhStat {
      numStarts
      numFirst
      numSecond
      numThird
      numFourth
      numFifth
      ven
      trk
      dist
      stakeWon
    }
  }
}"""

# HKJC 只有两个主要赛马场
KNOWN_VENUES = ("ST", "HV")

# 骑师名去负磅标记: "鍾易禮 (-2)" → "鍾易禮"
JOCKEY_CLEAN_PATTERN = re.compile(r"\s*[\(（][+-]?\d+[\)）]\s*")
# 马名去烙号后缀: "極速神影(L204)" → "極速神影"
HORSE_BRAND_PATTERN = re.compile(r"\([A-Z]\d+\)$")
# 中文骑师名验证（2-4个连续中文字符）
CHINESE_JOCKEY = re.compile(r"^[\u4e00-\u9fff]{2,4}$")

# ==================== HTTP 客户端 ====================

# 全局 Session（模块级），保持连接复用
_session: Optional[requests.Session] = None
_USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)


def _get_session() -> requests.Session:
    """获取或创建全局 Session"""
    global _session
    if _session is None:
        _session = requests.Session()
        _session.headers.update({
            "User-Agent": _USER_AGENT,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "zh-HK,zh;q=0.9,en;q=0.8",
            "Accept-Encoding": "gzip, deflate",
            "Connection": "keep-alive",
        })
    return _session


def _fetch_page(url: str, params: dict, retries: int = 3,
                base_delay: float = 1.0, timeout: int = 20) -> Optional[str]:
    """带指数退避重试的页面请求

    Args:
        url: 请求 URL
        params: 查询参数
        retries: 最大重试次数
        base_delay: 首次等待秒数（后续翻倍）
        timeout: 单次请求超时秒数

    Returns:
        HTML 文本，失败返回 None
    """
    session = _get_session()
    last_exc: Optional[Exception] = None

    for attempt in range(retries):
        try:
            resp = session.get(url, params=params, timeout=timeout)
            resp.raise_for_status()
            resp.encoding = "utf-8"
            return resp.text
        except (requests.RequestException, ConnectionError) as exc:
            last_exc = exc
            if attempt < retries - 1:
                wait = base_delay * (2 ** attempt)
                logger.debug("[HTTP] Fetch %s failed (attempt %d/%d): %s, retrying in %.1fs" % (
                    url, attempt + 1, retries, exc, wait))
                time.sleep(wait)

    logger.warning("Fetch %s failed after %d attempts: %s" % (url, retries, last_exc))
    return None


# ==================== 清洗工具 ====================
def _clean_jockey(name: str) -> str:
    """清洗骑师名，去除负磅标记如 '鍾易禮 (-2)' → '鍾易禮'"""
    if not name:
        return ""
    return JOCKEY_CLEAN_PATTERN.sub("", name).strip()


def _clean_horse_name(name: str) -> str:
    """清洗马名，去除烙号后缀如 '極速神影(L204)' → '極速神影'"""
    if not name:
        return ""
    return HORSE_BRAND_PATTERN.sub("", name).strip()


# ==================== 页面解析 ====================
def _discover_venue_and_races(html: str) -> tuple:
    """从 results 页面 HTML 中提取 venue 和赛事编号列表

    匹配模式覆盖 HKJC 页面可能出现的多种 URL 格式:
      - Racecourse=ST & RaceNo=3  (新格式)
      - RaceCourse=HV & Race_No=2 (变体)
      - 直接在 form action 或 JavaScript 中出现

    Returns:
        (venue_code 或 None, [race_numbers])
    """
    venue = None
    race_nos = []

    # 尝试多种 venue 格式
    venue_patterns = [
        r"[&?]Racecourse=([A-Z]{2})",
        r"[&?]RaceCourse=([A-Z]{2})",
        r"Racecourse\s*[:=]\s*[\"']([A-Z]{2})[\"']",
        r"racecourse[\"']?\s*[:=]\s*[\"']?([A-Z]{2})[\"']?",
    ]
    for pat in venue_patterns:
        m = re.search(pat, html)
        if m:
            venue = m.group(1).upper()
            if venue in KNOWN_VENUES:
                break

    # 尝试多种 RaceNo 格式
    race_patterns = [
        r"[&?]RaceNo=(\d+)",
        r"[&?]Race_No=(\d+)",
        r"RaceNo\s*[:=]\s*[\"'](\d+)[\"']",
    ]
    for pat in race_patterns:
        nums = re.findall(pat, html)
        if nums:
            race_nos = sorted(set(int(n) for n in nums))
            break

    return venue, race_nos


def _parse_results_table(html: str, race_no: int) -> list:
    """解析赛果页单场数据

    页面结构（results 页单场）:
      tbody[0] = 赛事元信息（場地狀況 等）— 跳过
      tbody[1] = 赛果主表（12列: 名次|馬號|馬名|騎師|練馬師|負磅|馬重|排檔|時間差|...）
      tbody[2] = 独赢/位置赔率 — 跳过
      tbody[3] = 赛事报告表（4列: 名次|馬號|馬名|报告文本）— 跳过

    区分策略: 列数量（赛果表 ≥ 10 列，报告表 4 列）
             或骑师名是否为纯中文名

    输出策略: 每匹马输出两条映射 — 騎師版 + 練馬師版
      騎師版: 用于 JKC 骑师王 (transformer lookup key = race_no + 骑师名)
      練馬師版: 用于 TNC 练马师王 (transformer lookup key = race_no + 练马师名)
      冲突键是 (race_no, jockey_name)，騎師名和練馬師名不同，不会互冲。
    """
    soup = BeautifulSoup(html, "lxml")
    mappings = []
    seen = set()  # 去重: (race_no, horse_no, name, role)

    for tbody in soup.find_all("tbody"):
        rows = tbody.find_all("tr")
        if len(rows) < 3:
            continue

        for row in rows:
            cells = row.find_all(["td", "th"])
            if len(cells) < 5:
                continue

            cell_texts = [c.get_text(strip=True) for c in cells]

            # 赛果表特征: 第1列(名次)和第2列(馬號)都是数字
            if not (cell_texts[0].isdigit() and cell_texts[1].isdigit()):
                continue

            horse_no = cell_texts[1]
            horse_name = _clean_horse_name(cell_texts[2])

            # 列3=騎師, 列4=練馬師
            jockey_name = _clean_jockey(cell_texts[3])
            trainer_name = _clean_jockey(cell_texts[4]) if len(cell_texts) >= 5 else ""

            # 赛果表: 騎師名必须是 2-4 个中文字（排除赛事报告表的长描述）
            if not jockey_name or not CHINESE_JOCKEY.match(jockey_name):
                continue

            # 输出 ① 騎師版映射
            jk = (race_no, horse_no, jockey_name, "j")
            if jk not in seen:
                seen.add(jk)
                mappings.append({
                    "race_no": race_no,
                    "horse_no": horse_no,
                    "horse_name": horse_name,
                    "jockey_name": jockey_name,
                    # 赛果页无「優先參賽次序」列（仅排位表有），降级路径标记恒为空
                    "priority_mark": "",
                })

            # 输出 ② 練馬師版映射（列4）
            if trainer_name and CHINESE_JOCKEY.match(trainer_name) and trainer_name != jockey_name:
                tk = (race_no, horse_no, trainer_name, "t")
                if tk not in seen:
                    seen.add(tk)
                    mappings.append({
                        "race_no": race_no,
                        "horse_no": horse_no,
                        "horse_name": horse_name,
                        "jockey_name": trainer_name,  # DB 字段复用: 练马师名也存在这里
                        "priority_mark": "",
                    })

    return mappings


# ==================== 排位表解析 ====================

def _expected_cn_date(meeting_date: str) -> str:
    """meeting_date 'YYYY-MM-DD' → 排位表页面日期显示格式 '2026年9月6日'（月日无前导零）

    用于校验返回页面是否为请求日期：请求无赛事的日期时 HKJC 会返回默认页面，
    页面日期与请求不符即放弃该路径。
    """
    try:
        y, m, d = meeting_date.split("-")
        return "%d年%d月%d日" % (int(y), int(m), int(d))
    except ValueError:
        return ""


def _discover_racecard_races(html: str) -> list:
    """从排位表页面提取场次编号集合（页面上的 RaceNo=N 场次切换链接）"""
    nums = re.findall(r"RaceNo=(\d+)", html)
    return sorted(set(int(n) for n in nums))


def _link_in_next_tds(td, pattern: str, limit: int = 12):
    """在马名格之后的前 limit 个兄弟格中查找特征链接（騎師/練馬師）

    限制在有限兄弟格内查找，避免越界串到下一个马匹条目或页面其他区域的链接。
    limit=12：常规页面練馬師在馬名后第 6 格（烙號/負磅/騎師/可能超磅/檔位/練馬師），
    但部分场次存在额外列（見習讓磅等）会把練馬師推后，8 格边界曾导致练马师链接漏抓。
    """
    for sib in td.find_next_siblings("td")[:limit]:
        a = sib.find("a", href=re.compile(pattern))
        if a:
            return a
    return None


def _find_racecard_priority_index(soup) -> tuple:
    """定位排位表主表表头中「馬名」与「優先參賽次序」的列下标

    页面 DOM 说明: 主表表头是一个独立 tr，直接子格为各列表头（含 馬名/騎師/練馬師/優先參賽次序）；
    页面里另有一行把表头+全部马匹内容合并为子孙节点的行（直接子格不含完整列名），
    以及「後備馬匹」小表（表头含 馬名/優先參賽次序 但无 騎師），
    用「直接子格包含 馬名+騎師+優先參賽次序」唯一锁定主表表头。

    Returns:
        (馬名列下标, 優先參賽次序列下标)；页面无该列（表头结构变化）时对应项为 None
    """
    for tr in soup.find_all("tr"):
        cells = tr.find_all(["th", "td"], recursive=False)
        if len(cells) < 5:
            continue
        texts = [c.get_text(strip=True) for c in cells]
        if "馬名" in texts and "騎師" in texts and "優先參賽次序" in texts:
            return texts.index("馬名"), texts.index("優先參賽次序")
    return None, None


def _parse_racecard(html: str, race_no: int) -> list:
    """解析排位表单场页面

    页面结构（每个出赛马匹条目，列序固定）:
      <td>馬號</td> <td>6次近績</td> <td>綵衣img</td>
      <td><a href="...horse?horseid=...">馬名</a></td>
      <td>烙號</td> <td>負磅</td>
      <td><a href="...jockeyprofile?...">騎師</a></td>
      <td>可能超磅</td> <td>檔位</td>
      <td><a href="...trainerprofile?...">練馬師</a></td> ...

    解析策略: 以 horse?horseid= 链接为锚点 →
      - 馬名 = 链接文本
      - 馬號 = 锚点格向前兄弟格中最远（行首方向）的纯数字格
        （「6次近績」列单数字时取第一个会错号，如 2026-09-13 R2
        好力近績 '9' 被误当馬號致同场 9 号重叠）
      - 騎師 / 練馬師 = 锚点格之后有限兄弟格内的特征链接
      - 優先參賽次序 = 按主表表头列下标对位读取（个别场次存在额外列如見習讓磅，
        馬名后的固定偏移会漂移，表头索引对位不受影响；并以「馬名格在行内的位置
        与表頭馬名列一致」自校验，不一致时告警留空）
    页面底部「退出馬匹」区（TdScratch 标记）无骑师链接，天然被排除。

    優先參賽次序标记含义（页面官方注释）:
      "+ 王牌 / * 獲得優先出賽之馬匹 / 1,2,3... 練馬師之馬匹優先參賽次序"；
      存原始值并归一化去空格（"+ 1" → "+1"，"-" 视为无标记存空串）。

    输出策略: 与 LocalResults 解析一致 — 每匹马输出 騎師版 + 練馬師版 两条映射。
    """
    soup = BeautifulSoup(html, "lxml")
    mappings = []
    seen = set()  # 去重: (race_no, horse_no, name, role)

    name_idx, pri_idx = _find_racecard_priority_index(soup)
    if pri_idx is None:
        logger.warning("排位表 Race %02d: 表头未找到「優先參賽次序」列，priority_mark 全部留空" % race_no)
    horse_links = soup.find_all("a", href=re.compile(r"horse\?horseid="))
    for a in horse_links:
        td = a.find_parent("td")
        if td is None:
            continue

        horse_name = _clean_horse_name(a.get_text(strip=True))
        if not horse_name:
            continue

        # 騎師 / 練馬師：无骑师链接的条目（退出馬匹区）直接跳过
        jockey_a = _link_in_next_tds(td, r"jockeyprofile")
        trainer_a = _link_in_next_tds(td, r"trainerprofile")
        jockey_name = _clean_jockey(jockey_a.get_text(strip=True)) if jockey_a else ""
        trainer_name = _clean_jockey(trainer_a.get_text(strip=True)) if trainer_a else ""
        if not jockey_name or not CHINESE_JOCKEY.match(jockey_name):
            continue

        # 馬號：向前最多找 5 个兄弟格，取离馬名最远（行首方向）的纯数字格。
        # 不可取第一个：紧邻馬名的「6次近績」列在马匹仅出赛过一次时是单个纯数字
        # （如 2026-09-13 R2 好力近績 '9'、盈智多寶近績 '10'），会被误当馬號，
        # 造成同场马号重叠 + 真实馬號整匹缺失；馬號是行首第一个纯数字格
        horse_no = ""
        for sib in td.find_previous_siblings("td")[:5]:
            t = sib.get_text(strip=True)
            if t.isdigit():
                horse_no = t  # 不 break，持续覆盖 → 最终保留最远（行首）的数字格
        if not horse_no:
            continue

        # 優先參賽次序：行内直接 td 按表头下标对位（先自校验馬名列对齐）
        priority_mark = ""
        if pri_idx is not None:
            row_tds = td.find_parent("tr").find_all("td", recursive=False)
            if name_idx is not None and td in row_tds and row_tds.index(td) != name_idx:
                logger.warning("排位表 Race %02d %s: 馬名格行内位置(%d)与表頭(%d)不一致，priority_mark 留空" % (
                    race_no, horse_name, row_tds.index(td), name_idx))
            elif pri_idx < len(row_tds):
                priority_mark = re.sub(r"\s+", "", row_tds[pri_idx].get_text(strip=True))
                if priority_mark == "-":
                    priority_mark = ""
            else:
                logger.warning("排位表 Race %02d %s: 行内格数(%d)不足優先參賽次序列下标(%d)，priority_mark 留空" % (
                    race_no, horse_name, len(row_tds), pri_idx))

        # 输出 ① 騎師版映射
        jk = (race_no, horse_no, jockey_name, "j")
        if jk not in seen:
            seen.add(jk)
            mappings.append({
                "race_no": race_no,
                "horse_no": horse_no,
                "horse_name": horse_name,
                "jockey_name": jockey_name,
                "priority_mark": priority_mark,
            })

        # 输出 ② 練馬師版映射（DB 字段复用: 练马师名也存在 jockey_name）
        if trainer_name and CHINESE_JOCKEY.match(trainer_name) and trainer_name != jockey_name:
            tk = (race_no, horse_no, trainer_name, "t")
            if tk not in seen:
                seen.add(tk)
                mappings.append({
                    "race_no": race_no,
                    "horse_no": horse_no,
                    "horse_name": horse_name,
                    "jockey_name": trainer_name,
                    "priority_mark": priority_mark,
                })

    return mappings


# ==================== 抓取主逻辑 ====================

def _scrape_all_races(race_date: str, venue: str, race_nos: list,
                      sleep_between: float = 0.6) -> list:
    """逐场抓取 results 页面并解析

    Args:
        race_date: 格式 "YYYY/MM/DD"
        venue: "ST" 或 "HV"
        race_nos: 赛事编号列表，如 [1,2,...,11]
        sleep_between: 每场之间的休眠秒数

    Returns:
        合并去重后的映射列表
    """
    all_mappings = []
    total = len(race_nos)

    for i, race_no in enumerate(race_nos):
        params = {
            "RaceDate": race_date,
            "Racecourse": venue,
            "RaceNo": str(race_no),
        }
        html = _fetch_page(RESULTS_URL, params)
        if html is None:
            logger.warning("Race %02d fetch failed (venue=%s)" % (race_no, venue))
            if i < total - 1:
                time.sleep(sleep_between)
            continue

        mappings = _parse_results_table(html, race_no)
        all_mappings.extend(mappings)
        logger.debug("[HTTP]   Race %02d (%s): %d mappings" % (race_no, venue, len(mappings)))

        if i < total - 1:
            time.sleep(sleep_between)

    return all_mappings


def _scrape_racecard(race_date: str, meeting_date: str,
                     sleep_between: float = 0.6) -> list:
    """逐场抓取排位表页面并解析（赛前赛后均可用的首选路径）

    页面日期与请求日期不符（该日无赛事被重定向到默认页面）时返回空，
    由上层降级到 LocalResults 路径。
    """
    html = _fetch_page(RACECARD_URL, {"RaceDate": race_date})
    if html is None:
        return []

    expected = _expected_cn_date(meeting_date)
    if expected and expected not in html:
        logger.info("[CRAWL] 排位表页面日期与 %s 不符（该日可能无赛事），跳过排位表路径" % meeting_date)
        return []

    race_nos = _discover_racecard_races(html)
    if not race_nos:
        logger.info("[CRAWL] 排位表页面未发现场次链接，尝试 1-11")
        race_nos = list(range(1, 12))

    all_mappings = []
    total = len(race_nos)
    for i, race_no in enumerate(race_nos):
        # 不复用首页 HTML：无 RaceNo 参数时过去的日期可能默认展示最后一场
        page = _fetch_page(RACECARD_URL, {"RaceDate": race_date, "RaceNo": str(race_no)})
        if page is None:
            logger.warning("排位表 Race %02d fetch failed" % race_no)
        else:
            mappings = _parse_racecard(page, race_no)
            all_mappings.extend(mappings)
            logger.debug("[HTTP]   排位表 Race %02d: %d mappings" % (race_no, len(mappings)))
        if i < total - 1:
            time.sleep(sleep_between)
    return all_mappings


def scrape_horse_mappings(meeting_date: str) -> list:
    """从 HKJC 抓取指定日期的骑师↔马匹对应关系

    抓取链路（逐层降级）:
      1. 排位表 racecard 逐场抓取（赛前赛后均可用；未开赛时 LocalResults
         对未来日期返回默认页面，会抓到重复的脏数据，故排位表必须优先）
      2. results 首页发现 venue + race_nos → 逐场抓取
      3. 常见 venue (ST/HV) 试猜，R1 探路 → 成功后逐场
      4. 全 venue × 全 race 暴力遍历

    Args:
        meeting_date: 赛事日期，格式 YYYY-MM-DD

    Returns:
        [{race_no, horse_no, horse_name, jockey_name, priority_mark}, ...]
    """
    race_date = meeting_date.replace("-", "/")
    logger.info("[CRAWL] 开始抓取 %s 的马匹映射..." % meeting_date)
    # ---------- 路径 1: 排位表 ----------
    mappings = _scrape_racecard(race_date, meeting_date)
    if mappings:
        logger.info("[CRAWL] ✓ 路径1(排位表)成功: %d 条映射" % len(mappings))
        return mappings

    # ---------- 路径 2: results 首页发现 ----------
    html = _fetch_page(RESULTS_URL, {"RaceDate": race_date})
    venue, race_nos = (None, [])
    if html:
        venue, race_nos = _discover_venue_and_races(html)

    if venue and race_nos:
        logger.info("[CRAWL] 路径2发现 venue=%s, races=%d" % (venue, len(race_nos)))
        mappings = _scrape_all_races(race_date, venue, race_nos)
        if mappings:
            logger.info("[CRAWL] ✓ 路径2成功: %d 条映射 (venue=%s)" % (len(mappings), venue))
            return mappings
    elif venue:
        logger.info("[CRAWL] 路径2 venue=%s, 但 race_nos 未发现，尝试 1-15" % venue)
        mappings = _scrape_all_races(race_date, venue, list(range(1, 16)))
        if mappings:
            logger.info("[CRAWL] ✓ 路径2扩展成功: %d 条映射" % len(mappings))
            return mappings

    # ---------- 路径 3: 常见 venue 试猜 ----------
    for venue in KNOWN_VENUES:
        logger.debug("[HTTP] 路径3 试 venue=%s, R1..." % venue)
        r1_html = _fetch_page(RESULTS_URL, {
            "RaceDate": race_date,
            "Racecourse": venue,
            "RaceNo": "1",
        })
        if not r1_html:
            continue

        r1_mappings = _parse_results_table(r1_html, 1)
        if not r1_mappings:
            continue

        logger.info("[CRAWL] 路径3 venue=%s R1 有 %d 条，继续逐场..." % (venue, len(r1_mappings)))
        all_mappings = list(r1_mappings)
        for race_no in range(2, 16):
            time.sleep(0.5)
            params = {
                "RaceDate": race_date,
                "Racecourse": venue,
                "RaceNo": str(race_no),
            }
            html = _fetch_page(RESULTS_URL, params)
            if not html:
                break  # 后续都没数据，停止
            mappings = _parse_results_table(html, race_no)
            if not mappings:
                break
            all_mappings.extend(mappings)

        if all_mappings:
            logger.info("[CRAWL] ✓ 路径3成功: %d 条映射 (venue=%s)" % (len(all_mappings), venue))
            return all_mappings

    # ---------- 路径 4: 全 venue × 全 race 暴力遍历 ----------
    logger.info("[CRAWL] 路径1/2/3 均失败，执行路径4暴力遍历...")
    for venue in KNOWN_VENUES:
        for race_no in range(1, 16):
            time.sleep(0.4)
            params = {
                "RaceDate": race_date,
                "Racecourse": venue,
                "RaceNo": str(race_no),
            }
            html = _fetch_page(RESULTS_URL, params)
            if not html:
                continue
            mappings = _parse_results_table(html, race_no)
            if mappings:
                # 找到有数据的 venue，以它为主
                logger.info("[CRAWL] 路径4 在 venue=%s race=%02d 找到数据，逐场抓取" % (venue, race_no))
                collected = []
                for rn in range(race_no, 16):
                    time.sleep(0.5)
                    html = _fetch_page(RESULTS_URL, {
                        "RaceDate": race_date,
                        "Racecourse": venue,
                        "RaceNo": str(rn),
                    })
                    if not html:
                        break
                    collected.extend(_parse_results_table(html, rn))
                if collected:
                    logger.info("[CRAWL] ✓ 路径4成功: %d 条映射" % len(collected))
                    return collected

    logger.warning("✗ 所有路径均未获取到 %s 的马匹映射" % meeting_date)
    return []


def _validate_mappings(mappings: list) -> None:
    """抓取结果结构性校验：每匹马应输出 騎師版 + 練馬師版 两条映射（按 (race_no, horse_no) 分组）。

    只有一条的马输出 warning（骑师版缺失通常整条被跳过；练马师版缺失多为列越界漏抓），
    便于及时发现映射缺失导致的看板马号/马名空白。
    """
    by_horse = {}
    for m in mappings:
        key = (m["race_no"], m["horse_no"])
        by_horse[key] = by_horse.get(key, 0) + 1
    orphans = sorted(k for k, c in by_horse.items() if c < 2)
    if orphans:
        detail = ", ".join("R%s#%s" % (r, h) for r, h in orphans[:20])
        more = " ...等共 %d 匹" % len(orphans) if len(orphans) > 20 else ""
        logger.warning("⚠ 校验: %d 匹马只有单边映射（骑师版或练马师版缺失）: %s%s" % (
            len(orphans), detail, more))


# ==================== 实时马匹数据（GraphQL） ====================

def fetch_live_runners(meeting_date: str) -> dict:
    """从 racing.hkjc.com 页面背后的 GraphQL 拉取指定日期每场出赛马匹的实时数据

    官方 query 按页面拆分（白名单限制，无法合并为一条）：騎師座騎页 query 提供
    馬名/騎師/狀態，練馬師馬匹页 query 提供 練馬師，各拉一次按 (场次,马号) 合并；
    獨贏赔率取会议级 pmPools(oddsTypes:[WIN])[].oddsNodes（runners[].winOdds
    在晨早赔率阶段恒为空，不可用）。两条 query 任一失败即返回空 dict——半边数据
    凑不齐 騎師版+練馬師版 两条映射，写入会触发结构漂移整马重写、误删另一版。

    返回 {race_no: {horse_no: {odds, name, jockey, trainer, scratched}}}:
      odds      獨贏(WIN)赔率原始字符串,赔率未发布(oddsValue '---')时为空串
      name      馬名(中文,已清洗烙号后缀)
      jockey    騎師名(已清洗负磅标记)
      trainer   練馬師名
      scratched runner.status 含 SCRATCH 即视为退出(清空赔率的信号)
    以下情况返回空 dict（不写入任何数据）：
      - 请求重试后仍失败 / GraphQL 返回错误
      - commonMeetings 未返回该日期会议（query 带 active:true，
        会议结束被清理后不再返回，天然防止把别的赛事日数据写入目标日期）
    赔率未发布时仍返回 runners（odds 为空串），供退出馬/騎師變更同步。
    """
    mtg_jockey = _fetch_meeting(JOCKEY_RIDES_GRAPHQL_QUERY, meeting_date)
    mtg_trainer = _fetch_meeting(TRAINERS_ENTRIES_GRAPHQL_QUERY, meeting_date)
    if mtg_jockey is None or mtg_trainer is None:
        return {}

    # 獨贏赔率: 两条 query 均返回 WIN 池 oddsNodes, 按 (场次,马号) 展开成映射。
    # combString 为补零马号("01"), 需 int 对齐 runner.no; '---' 表示赔率暂未发布
    odds_by_race = {}
    for mtg in (mtg_jockey, mtg_trainer):
        for pool in mtg.get("pmPools") or []:
            race_no = int((pool.get("leg") or {}).get("number") or 0)
            if race_no <= 0:
                continue
            for node in pool.get("oddsNodes") or []:
                try:
                    horse_no = str(int(str(node.get("combString") or "")))
                except ValueError:
                    continue
                odds = str(node.get("oddsValue") or "").strip()
                if odds and odds != "---":
                    odds_by_race.setdefault(race_no, {})[horse_no] = odds

    # 練馬師: 仅 TrainersEntries query 返回, 按 (场次,马号) 收集
    trainer_by_key = {}
    for race in mtg_trainer.get("races") or []:
        race_no = int(race.get("no") or 0)
        if race_no <= 0:
            continue
        for runner in race.get("runners") or []:
            horse_no = str(runner.get("no") or "").strip()
            if horse_no.isdigit():
                trainer_by_key[(race_no, horse_no)] = _clean_jockey(
                    (runner.get("trainer") or {}).get("name_ch") or "")

    # 馬名/騎師/狀態: 以 JockeysRides query 为主干构建 runners
    runners = {}
    for race in mtg_jockey.get("races") or []:
        race_no = int(race.get("no") or 0)
        if race_no <= 0:
            continue
        for runner in race.get("runners") or []:
            # 後備馬无正式馬號(如 S1),補位(继承退出馬馬號)前不同步
            horse_no = str(runner.get("no") or "").strip()
            if not horse_no.isdigit():
                continue
            status = str(runner.get("status") or "").upper()
            runners.setdefault(race_no, {})[horse_no] = {
                "odds": odds_by_race.get(race_no, {}).get(horse_no, ""),
                "name": _clean_horse_name((runner.get("horse") or {}).get("name_ch") or ""),
                "jockey": _clean_jockey((runner.get("jockey") or {}).get("name_ch") or ""),
                "trainer": trainer_by_key.get((race_no, horse_no), ""),
                "scratched": "SCRATCH" in status,
            }
    if runners:
        with_odds = sum(1 for h in runners.values() for r in h.values() if r["odds"])
        logger.info("[CRAWL] 实时马匹数据: %s/%s 拉到 %d 场 runners (獨贏赔率 %d 匹)" % (
            meeting_date, mtg_jockey.get("venueCode"), len(runners), with_odds))
    return runners


def _fetch_meeting(query: str, meeting_date: str) -> Optional[dict]:
    """POST 一条白名单 GraphQL query, 返回 commonMeetings 中 date 匹配的会议

    失败（重试后仍网络错误 / GraphQL 返回 errors）、无该日期会议时返回 None。
    """
    session = _get_session()
    body = None
    last_exc = None
    for attempt in range(3):
        try:
            resp = session.post(
                ODDS_GRAPHQL_URL,
                json={"query": query,
                      "variables": {"dates": [meeting_date]}},
                headers={"Content-Type": "application/json",
                         "Origin": "https://racing.hkjc.com",
                         "Referer": "https://racing.hkjc.com/"},
                timeout=20,
            )
            resp.raise_for_status()
            body = resp.json()
            break
        except (requests.RequestException, ValueError) as exc:
            last_exc = exc
            if attempt < 2:
                time.sleep(1.0 * (2 ** attempt))
    if body is None:
        logger.warning("实时马匹数据 GraphQL 请求失败: %s" % last_exc)
        return None
    if body.get("errors"):
        logger.warning("实时马匹数据 GraphQL 返回错误: %s" % str(body["errors"])[:200])
        return None
    for mtg in (body.get("data") or {}).get("commonMeetings") or []:
        # 会议校验：按 date 精确匹配（query 已传 dates 过滤，双保险防串日）
        if mtg.get("date") == meeting_date:
            return mtg
    logger.info("[CRAWL] 实时马匹数据: 未返回 %s 会议数据(会议不存在或已被清理)" % meeting_date)
    return None


# ==================== 编排与挂钩入口 ====================

def scrape_and_store(meeting_date: str) -> int:
    """抓取并存储指定日期的马匹映射，返回存储行数
    映射入库后顺带做一次实时同步（獨贏赔率/退出馬/騎師變更；赛日 WIN 池
    开售后有值；预售日晨早赔率未发布时 odds 为空串，不覆盖已有值）。
    """
    mappings = scrape_horse_mappings(meeting_date)
    if mappings:
        _validate_mappings(mappings)
        count = store_horse_mappings(meeting_date, mappings)
        logger.info("[CRAWL] 存储 %s: %d 行 (抓 %d 条)" % (meeting_date, count, len(mappings)))
        runners = fetch_live_runners(meeting_date)
        if runners:
            _log_live_sync(meeting_date, *sync_runners_live(meeting_date, runners))
        return count
    return 0


def _log_live_sync(meeting_date: str, updated: int, inserted: int, cleared: int) -> None:
    """实时同步结果日志：有变化才打一行，避免每轮空转刷屏"""
    if updated or inserted or cleared:
        logger.info("[CRAWL] 实时马匹同步 %s: 赔率更新 %d 行 新增 %d 行 退出清空 %d 行" % (
            meeting_date, updated, inserted, cleared))


def ensure_horse_mapping(meeting_date: str) -> None:
    """挂钩入口①（jkc_tnc_crawler 启动阶段调用）：确保指定日期有马匹映射

    保护规则：映射只在库中无数据时抓取——重抓会整体替换（DELETE+INSERT），
    而赛日 HKJC 排位表会移除「優先參賽次序」列，+1 等标记会丢失。
    失败不抛出，仅 WARN，不阻断 JKC/TNC 轮询主流程。
    """
    try:
        total, _with_odds = horse_mapping_stats(meeting_date)
        if total > 0:
            logger.info("[CRAWL] 马匹映射已存在 %s: %d 行，跳过抓取" % (meeting_date, total))
            return
        logger.info("[CRAWL] 库中无 %s 马匹映射，开始抓取..." % meeting_date)
        count = scrape_and_store(meeting_date)
        logger.info("[CRAWL] 马匹映射抓取完成 %s: %d 行" % (meeting_date, count))
    except Exception as error:
        logger.warning("马匹映射抓取失败 %s(不阻断 JKC/TNC 轮询): %s" % (meeting_date, error), exc_info=error)

def sync_horse_runners_if_needed(meeting_date: str) -> bool:
    """挂钩入口②（jkc_tnc_crawler 轮询循环调用）：马匹数据实时同步

    由主循环按独立赔率节拍调用（--odds-interval，默认 10 秒），拉 GraphQL 实时
    runners 增量同步 race_horse_mapping：獨贏赔率覆盖更新、新馬（後備補位）插入、
    退出馬清空赔率、騎師/練馬師/馬名變更重写映射行（保留優先參賽次序标记）。
    不重抓 HTML 映射（重抓会整体替换，丢失優先參賽次序标记）。
    映射不存在时跳过（ensure_horse_mapping 负责抓映射）；失败不抛出，仅 WARN。
    返回是否真正执行了同步（False=映射缺失/无会议数据/请求失败，供调用方退避节流）。
    """
    try:
        total, _with_odds = horse_mapping_stats(meeting_date)
        if total == 0:
            return False  # 映射都没有，同步无处可写（ensure_horse_mapping 负责抓映射）
        runners = fetch_live_runners(meeting_date)
        if runners:
            _log_live_sync(meeting_date, *sync_runners_live(meeting_date, runners))
            return True
        return False  # 无会议数据（会议结束被清理/请求失败）
    except Exception as error:
        logger.warning("实时马匹同步失败 %s: %s" % (meeting_date, error), exc_info=error)
        return False

# ==================== CLI 入口 ====================

USAGE_TEXT = "\n".join([
    "用法: python3 horse_mapping_crawler.py --date YYYY-MM-DD [--refresh-odds] [--log-file 路径]",
    "",
    "  --date          目标赛日(HK 日期)，格式 YYYY-MM-DD",
    "  --refresh-odds  仅同步实时马匹数据(獨贏赔率/退出馬/新馬;不重抓映射,映射缺失时先自动抓映射)",
    "  --log-file      日志文件路径，缺省写脚本目录 logs/horse_YYYY-MM-DD.log 并每日自动切分",
    "",
    "默认行为: 库中无该日映射时抓取入库(排位表→赛果页 4 层降级)，已有映射则跳过",
    "         (重抓会整体替换，赛日 HKJC 已移除「優先參賽次序」列，+1 标记会丢失)",
    "数据库连接: 环境变量 PGHOST/PGPORT/PGDATABASE/PGUSER/PGPASSWORD(见 database/connection.py)",
])


def _parse_args(argv):
    date = ""
    refresh_odds = False
    log_file = None

    i = 0
    while i < len(argv):
        arg = argv[i]

        def next_value():
            nonlocal i
            i += 1
            if i >= len(argv):
                sys.stderr.write("错误: 参数 %s 缺少值\n%s\n" % (arg, USAGE_TEXT))
                sys.exit(1)
            return argv[i]

        if arg == "--date":
            date = next_value()
        elif arg == "--refresh-odds":
            refresh_odds = True
        elif arg == "--log-file":
            log_file = next_value()
        elif arg in ("--help", "-h"):
            print(USAGE_TEXT)
            sys.exit(0)
        else:
            sys.stderr.write("错误: 未知参数 %s\n%s\n" % (arg, USAGE_TEXT))
            sys.exit(1)
        i += 1

    if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", date):
        sys.stderr.write("错误: --date 格式应为 YYYY-MM-DD，收到 %s\n%s\n" % (date, USAGE_TEXT))
        sys.exit(1)
    return date, refresh_odds, log_file


def main(argv):
    date, refresh_odds, log_file = _parse_args(argv)

    # 日志初始化:控制台 + 文件双输出;缺省写脚本目录 logs/horse_YYYY-MM-DD.log,每日自动切分
    session_id, log_file = setup_logging(log_file, file_prefix="horse")
    logger.info("[CRAWL] 马匹爬虫启动 date=%s refresh-odds=%s" % (date, refresh_odds))
    logger.info("[CRAWL] 日志会话 sid=%s 文件=%s" % (session_id, log_file))
    if refresh_odds:
        # 仅同步实时数据:映射缺失时先抓齐(已有则跳过),再强制拉一次 GraphQL
        ensure_horse_mapping(date)
        runners = fetch_live_runners(date)
        if runners:
            _log_live_sync(date, *sync_runners_live(date, runners))
        else:
            logger.info("[CRAWL] 实时马匹数据 %s 无数据(会议不存在或已被清理)" % date)
    else:
        ensure_horse_mapping(date)


if __name__ == "__main__":
    try:
        main(sys.argv[1:])
    except KeyboardInterrupt:
        logger.critical("KeyboardInterrupt")
        sys.exit(1)
    except Exception:
        logger.critical(traceback.format_exc())
        sys.exit(1)
