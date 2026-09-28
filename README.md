# HKJCdemo — 香港赛马会 JKC/TNC 赔率爬虫

轮询 HKJC GraphQL 接口，采集 JKC/TNC 玩法彩池快照与马匹獨贏赔率，落库 PostgreSQL（按赛日分区）。

## 启动命令

```bash
python3 jkc_tnc_crawler.py --date 2026-09-13（日期） --venue ST（地点） --stop-stable 999999 --max-hours 999999
```

- `--date` 目标赛日（HK 日期，`YYYY-MM-DD`），缺省自动探测今天起 4 天内首个含 JKC/TNC 的赛日
- `--venue` 场地代码：`ST` 沙田 / `HV` 跑马地 / `S1` / `S2`，默认 `ST`
- `--stop-stable 999999` 关闭"派彩后稳定即停"，靠 `--max-hours` 兜底退出
- `--max-hours 999999` 关闭最长运行时长的安全上限，持续运行直到手动停止

## 快速开始

```bash
# 1. 安装依赖
pip3 install -r requirements.txt

# 2. 配置数据库连接(项目根目录 .env;环境变量优先,.env 只补缺)
cat > .env <<'EOF'
PGHOST=127.0.0.1
PGPORT=5432
PGDATABASE=horse
PGUSER=chen
PGPASSWORD=你的密码
EOF

# 3. 启动(新机器无需手动建库建表,启动时自动完成)
python3 jkc_tnc_crawler.py --date 2026-09-13 --venue ST --stop-stable 999999 --max-hours 999999
```

启动时自动执行幂等自检：建库（如缺失）→ 建表/索引/约束 → 确保目标赛日分区存在。
要求 `PGUSER` 有 `createdb` 权限；**仅自动建结构，不迁移历史数据**。

## 全部参数

| 参数 | 默认 | 说明 |
|---|---|---|
| `--date` | 自动探测 | 目标赛日（HK 日期） |
| `--venue` | `ST` | 场地代码，与落库 `venue_code` 一致（ST/HV/S1/S2） |
| `--interval` | `60000` | JKC/TNC 快照轮询间隔（毫秒） |
| `--odds-interval` | `10000` | 赔率同步间隔（毫秒），与 `--interval` 解耦 |
| `--once` | - | 单次快照模式：轮询一轮后退出 |
| `--stop-stable` | `3` | PAYOUTSTARTED 后 payload 连续不变的判定轮数，超过即停止 |
| `--max-hours` | `30` | 最长运行小时数（安全上限） |
| `--table` | `graphql_jkc_tnc_nodes` | 写入表名 |
| `--log-file` | `logs/crawl_YYYY-MM-DD.log` | 日志路径，缺省按 HK 自然日切分 |
| `--heartbeat` | `10` | 静默期心跳间隔（分钟），`0` 关闭 |

## 目录结构

```
crawler_py/
├── jkc_tnc_crawler.py        # 主爬虫:JKC/TNC 彩池轮询落库(入口)
├── horse_mapping_crawler.py  # 马匹映射抓取(排位表→赛果页 4 层降级)
├── logger.py                 # 按 HK 自然日切分的日志
├── database/
│   ├── connection.py         # 配置读取 + 全局单例连接 get_conn() / 短事务 transaction()
│   ├── init_db.py            # 启动自检:建库/建表/建分区(幂等)
│   ├── models.py             # ORM 模型(graphql_jkc_tnc_nodes 分区表 / race_horse_mapping)
│   └── horse_mapping_repo.py # race_horse_mapping 表操作(存储/实时同步/状态查询)
├── requirements.txt
└── .env                      # 数据库连接配置(不入库)
```

## 数据表

- **`graphql_jkc_tnc_nodes`** — 赔率采样主表，按 `meeting_date` RANGE 分区（日分区 + DEFAULT 兜底），复合主键 `(meeting_date, id)`，唯一约束防重
- **`race_horse_mapping`** — 马匹映射表：马号/马名/骑师/優先參賽次序/獨贏赔率，主键 `(meeting_date, race_no, jockey_name, horse_no)`
