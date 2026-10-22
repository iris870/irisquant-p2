# P2：全合约持续跟踪层 — 落地方案 v2.2（定稿版）

> 版本：v2.2 | 日期：2026-08-01
> 基线：v2.1（Q1-Q20 全部修正）
> 状态：✅ 定稿（6 项待确认已全部确认），待开工阶段 1

## 变更记录（相对 v2.1）

| # | 变更 | 来源 | 说明 |
|---|------|------|------|
| R1 | 提前转正 | 确认项 2 | observing 期内触发 strong_momentum → 立即 active，不等 48h |
| R2 | HIT_THRESHOLD 按信号类型差异化 | 确认项 3 | 每类信号独立命中阈值 |
| R3 | signal_engine 跑完 sys.exit(0) | 确认项 4 | 配合 PM2 cron_restart + autorestart:false |
| R4 | 健康检查单行 JSON（status ok/critical） | 确认项 5 | 便于 jq 解析与后续接通道 |
| R5 | observing 币也全量算信号 | 一致性修正 | 否则 R1 永远不触发（见下） |

### R5 说明（关键一致性）

用户确认项 2 要求"observing 期触发 strong_momentum → 提前转正"。但 v2.1 设计是"observing 期采集数据但不发信号（防污染）"——observing 币没有 price_surge/oi_spike 信号，就永远不可能触发 strong_momentum，提前转正成为死代码。

修正：**信号引擎对 active + observing 全部算信号**；防污染的职责移到消费层——Screener 加分前 join contract_meta 只认 status='active' 的币。这样：
- observing 币的信号可驱动状态机（提前转正）
- observing 币的信号不会进入交易链路（消费层过滤）
- new_listing 信号本来就该对 observing 币发

---

## 一、架构总览（4 层）

```
┌────────────────────────────────────────────────────────────┐
│ 第0层 发现与准入                                            │
│  每5min ticker 对比发现新 symbol（延迟 24h→5min）            │
│  exchangeInfo.onboardDate 区分存量老币/真新币                │
│  新币→observing(48h) + 立即发 new_listing 信号               │
│  转正：48h+OI≥10点+24h成交额≥2M 或 strong_momentum 提前转正(R1) │
│  404/退市 → inactive                                        │
└────────────────────────────────────────────────────────────┘
                            ↓
┌────────────────────────────────────────────────────────────┐
│ 第1层 采集（单 daemon，5min 周期）                           │
│  Ticker 5min → contracts_track + contracts_history(5min)   │
│  OI 15min（10 worker + 全局30req/s限流）→ open_interest 独立表│
│  Funding 8h（premiumIndex 全量）→ funding_history 独立表     │
│  深度 1h Top50（limit=20档）→ contracts_depth               │
│  WAL + 全连接 busy_timeout                                 │
│  F5 跳变：observing 跳过 + 连续3次强制入库                    │
└────────────────────────────────────────────────────────────┘
                            ↓
┌────────────────────────────────────────────────────────────┐
│ 第2层 信号引擎（每小时，跑完即退）                            │
│  一次 SQL 全量拉取 + 内存分组（1360次→3条查询）               │
│  7 类信号全实现（对 active+observing 全量算，R5）            │
│  signals 表带 direction（long/short/neutral）               │
│  应用层去重 + expires=max(dedup,4)h                         │
│  性能回填：1h/4h/24h 前信号回填 pnl/hit（R2 差异化阈值）       │
│  F7 signal_summary 监控日志 + sys.exit(0)（R3）              │
└────────────────────────────────────────────────────────────┘
                            ↓
┌────────────────────────────────────────────────────────────┐
│ 第3层 消费                                                  │
│  Screener：读 signals 加分，仅认 active 币（R5）             │
│  algochip.ai：持续跟踪 Tab + /api/p2/{track,signals,performance}│
│  健康检查 p2_healthcheck.py（cron 10min，单行 JSON，R4）      │
└────────────────────────────────────────────────────────────┘
```

---

## 二、数据模型（10 张表）

PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL;

-- 1. 合约元数据（状态机）
CREATE TABLE IF NOT EXISTS contract_meta (
    symbol          TEXT PRIMARY KEY,
    base_asset      TEXT,
    quote_asset     TEXT,
    listing_time    INTEGER,            -- onboardDate(ms)
    status          TEXT DEFAULT 'observing',  -- observing/active/inactive
    observing_since INTEGER,
    activated_at    INTEGER,
    last_oi_ts      INTEGER,
    source          TEXT DEFAULT 'binance',
    updated_at      INTEGER
);

-- 2. 最新基线（5min 覆盖）
CREATE TABLE IF NOT EXISTS contracts_track (
    symbol          TEXT PRIMARY KEY,
    price           REAL,
    price_change_24h REAL,
    quote_volume    REAL,
    open_interest   REAL,
    funding_rate    REAL,
    lsr             REAL,
    ts              INTEGER
);

-- 3. 5min 快照（热 7 天）→ 只含 ticker 数据
CREATE TABLE IF NOT EXISTS contracts_history (
    symbol          TEXT NOT NULL,
    ts              INTEGER NOT NULL,
    price           REAL,
    volume_24h      REAL,
    price_change_24h REAL,
    UNIQUE(symbol, ts)
);
CREATE INDEX IF NOT EXISTS idx_history_symbol_ts ON contracts_history(symbol, ts);
CREATE INDEX IF NOT EXISTS idx_history_ts ON contracts_history(ts);

-- 4. OI 序列（15min 粒度，保留 30 天）
CREATE TABLE IF NOT EXISTS open_interest (
    symbol          TEXT NOT NULL,
    ts              INTEGER NOT NULL,
    oi              REAL,
    UNIQUE(symbol, ts)
);
CREATE INDEX IF NOT EXISTS idx_oi_symbol_ts ON open_interest(symbol, ts);

-- 5. 费率序列（8h 粒度，保留 30 天）
CREATE TABLE IF NOT EXISTS funding_history (
    symbol          TEXT NOT NULL,
    ts              INTEGER NOT NULL,
    funding_rate    REAL,
    UNIQUE(symbol, ts)
);
CREATE INDEX IF NOT EXISTS idx_funding_symbol_ts ON funding_history(symbol, ts);

-- 5b. 多空比序列（15min 粒度，保留 30 天）
CREATE TABLE IF NOT EXISTS long_short_ratio (
    symbol          TEXT NOT NULL,
    ts              INTEGER NOT NULL,
    lsr             REAL,
    UNIQUE(symbol, ts)
);
CREATE INDEX IF NOT EXISTS idx_lsr_symbol_ts ON long_short_ratio(symbol, ts);

-- 6. 1h 降采样（温 30 天）
CREATE TABLE IF NOT EXISTS contracts_history_hourly (
    symbol          TEXT NOT NULL,
    hour_ts         INTEGER NOT NULL,
    avg_price       REAL,
    max_volume      REAL,
    avg_change_24h  REAL,
    PRIMARY KEY(symbol, hour_ts)
);

-- 7. 深度采样（1h，Top50）
CREATE TABLE IF NOT EXISTS contracts_depth (
    symbol          TEXT NOT NULL,
    ts              INTEGER NOT NULL,
    bid_depth_usd   REAL,
    ask_depth_usd   REAL,
    spread_pct      REAL,
    PRIMARY KEY(symbol, ts)
);

-- 8. 信号表（带 direction）
CREATE TABLE IF NOT EXISTS signals (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol      TEXT NOT NULL,
    signal_type TEXT NOT NULL,
    direction   TEXT DEFAULT 'long',    -- long/short/neutral
    strength    INTEGER DEFAULT 50,
    confidence  REAL DEFAULT 0.5,
    params      TEXT,                   -- JSON
    ts          INTEGER NOT NULL,
    expires_at  INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_signals_ts_type ON signals(ts, signal_type);
CREATE INDEX IF NOT EXISTS idx_signals_symbol_ts ON signals(symbol, ts);

-- 9. 信号快照（触发时基线，复盘用）
CREATE TABLE IF NOT EXISTS signal_snapshots (
    signal_id   INTEGER PRIMARY KEY,
    symbol      TEXT,
    price       REAL,
    oi          REAL,
    volume      REAL,
    funding     REAL,
    price_change REAL,
    ts          INTEGER
);

-- 10. 信号命中率（复盘闭环）
CREATE TABLE IF NOT EXISTS signal_performance (
    signal_id    INTEGER PRIMARY KEY,
    symbol       TEXT,
    signal_type  TEXT,
    direction    TEXT,
    entry_price  REAL,
    exit_price_1h REAL, exit_price_4h REAL, exit_price_24h REAL,
    pnl_1h REAL, pnl_4h REAL, pnl_24h REAL,
    hit_1h INTEGER, hit_4h INTEGER, hit_24h INTEGER,
    evaluated_at INTEGER
);

---

## 三、采集层 contracts_tracker.py（完整代码 v2.2）

```python
#!/usr/bin/env python3
"""P2 全合约持续跟踪 — 采集层 v2.2（Daemon）
频率: Ticker 5min | OI 15min(全局30req/s) | Funding 8h | 深度 1h | 合约同步 每日+5min实时发现
"""
import json, logging, sqlite3, threading, time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

import requests

DB_PATH = "/root/workspace/analysis_notes/market_radar.db"   # 绝对路径
FAPI = "https://fapi.binance.com"

# ── 集中常量区（阶段5调参只改这里）──
CYCLE         = 300          # ticker 5min
OI_EVERY      = 3            # OI 每 3 周期 = 15min
DEPTH_EVERY   = 12           # 深度每 12 周期 = 1h
OBSERVE_HOURS = 48           # 新币观察期
PROMOTE_OI_PTS= 10           # 转正: 24h 内 OI 采样点 ≥10
PROMOTE_VOL   = 2_000_000    # 转正: 24h 成交额 ≥ 2M USDT
JUMP_RATIO    = 0.5          # F5 跳变阈值 50%
JUMP_FORCE    = 3            # 连续 3 次跳变强制入库
OI_RPS        = 30           # 全局限流 30 req/s（Binance 上限 ~40）
OI_WORKERS    = 10
OI_TIMEOUT    = 120          # 整轮硬超时
DEPTH_TOP     = 50
DEPTH_LIMIT   = 20           # 深度档位

logger = logging.getLogger("p2-tracker")
_jump_count = {}                            # F5 逃生口状态
_throttle_lock, _throttle_last = threading.Lock(), [0.0]
_exchange_cache = {"ts": 0, "data": {}}


def connect() -> sqlite3.Connection:
    """每个连接都设 busy_timeout + synchronous=NORMAL（防 database is locked）"""
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.execute("PRAGMA busy_timeout=30000")
    conn.execute("PRAGMA synchronous=NORMAL")
    return conn


def init_db():
    """建全部 10 张表（幂等），部署即用；已有库做 ALTER 兼容迁移"""
    conn = connect()
    conn.execute("PRAGMA journal_mode=WAL")
    conn.executescript(SCHEMA)
    # 兼容迁移: 老库 contracts_track 无 lsr 列时补上
    cols = [r[1] for r in conn.execute("PRAGMA table_info(contracts_track)")]
    if "lsr" not in cols:
        conn.execute("ALTER TABLE contracts_track ADD COLUMN lsr REAL")
    conn.commit()
    conn.close()

def _throttle():
    """全局节流：所有 worker 共享锁，整体 ≤ OI_RPS req/s"""
    with _throttle_lock:
        dt = time.time() - _throttle_last[0]
        if dt < 1.0 / OI_RPS:
            time.sleep(1.0 / OI_RPS - dt)
        _throttle_last[0] = time.time()


# ── 合约发现与状态机 ──────────────────────

def fetch_onboard_dates() -> dict:
    """exchangeInfo 全量缓存 1h，返回 {symbol: onboardDate_ms}"""
    now = time.time()
    if now - _exchange_cache["ts"] > 3600:
        r = requests.get(f"{FAPI}/fapi/v1/exchangeInfo", timeout=30)
        r.raise_for_status()
        _exchange_cache["data"] = {s["symbol"]: s.get("onboardDate")
                                   for s in r.json()["symbols"] if s["status"] == "TRADING"}
        _exchange_cache["ts"] = now
    return _exchange_cache["data"]


def discover_new(conn, tickers, now_ms):
    """每 5min ticker 对比发现新币，立即进 observing（延迟 24h→5min）"""
    known = {r[0] for r in conn.execute("SELECT symbol FROM contract_meta")}
    onboard = fetch_onboard_dates()
    for t in tickers:
        sym = t["symbol"]
        if sym in known:
            continue
        conn.execute("""INSERT OR IGNORE INTO contract_meta
            (symbol, status, observing_since, listing_time, updated_at)
            VALUES (?, 'observing', ?, ?, ?)""",
            (sym, now_ms, onboard.get(sym), now_ms))
        logger.info("[NEW] %s -> observing (listing=%s)", sym, onboard.get(sym))
    conn.commit()


def promote_observing(conn, now_ms):
    """转正三通道：
    ① 常规: 观察≥48h + OI≥10点 + 24h成交额≥2M
    ② 提前(R1): observing 期内触发 strong_momentum → 立即 active
    """
    cutoff = now_ms - OBSERVE_HOURS * 3600000
    rows = conn.execute("""SELECT symbol FROM contract_meta
        WHERE status='observing' AND observing_since < ?""", (cutoff,)).fetchall()
    for (sym,) in rows:
        oi_pts = conn.execute("""SELECT COUNT(*) FROM open_interest
            WHERE symbol=? AND ts>?""", (sym, now_ms - 86400000)).fetchone()[0]
        vol = conn.execute("SELECT quote_volume FROM contracts_track WHERE symbol=?",
                           (sym,)).fetchone()
        vol = vol[0] if vol and vol[0] else 0
        if oi_pts >= PROMOTE_OI_PTS and vol >= PROMOTE_VOL:
            conn.execute("""UPDATE contract_meta SET status='active', activated_at=?
                WHERE symbol=?""", (now_ms, sym))
            logger.info("[ACTIVATE] %s (oi_pts=%d vol=%.0f)", sym, oi_pts, vol)

    # R1: 提前转正 —— observing 期触发 strong_momentum
    early = conn.execute("""SELECT DISTINCT s.symbol FROM signals s
        JOIN contract_meta m ON m.symbol = s.symbol
        WHERE m.status='observing' AND s.signal_type='strong_momentum'
        AND s.ts > m.observing_since""").fetchall()
    for (sym,) in early:
        conn.execute("UPDATE contract_meta SET status='active', activated_at=? WHERE symbol=?",
                     (now_ms, sym))
        logger.info("[EARLY-ACTIVATE] %s (strong_momentum)", sym)
    conn.commit()


def sync_contracts(conn, now_ms):
    """每日 00:05 全量同步：存量老币直接 active，退市币 inactive"""
    onboard = fetch_onboard_dates()
    now_s = time.time()
    for sym, listing in onboard.items():
        cur = conn.execute("SELECT status FROM contract_meta WHERE symbol=?", (sym,)).fetchone()
        if not cur:
            status = "active" if (listing and now_s * 1000 - listing > OBSERVE_HOURS * 3600000) else "observing"
            conn.execute("""INSERT OR IGNORE INTO contract_meta
                (symbol, status, observing_since, activated_at, listing_time, updated_at)
                VALUES (?, ?, ?, ?, ?, ?)""",
                (sym, status, now_ms, now_ms if status == "active" else None, listing, now_ms))
        else:
            conn.execute("UPDATE contract_meta SET listing_time=?, updated_at=? WHERE symbol=?",
                         (listing, now_ms, sym))
    conn.execute("""UPDATE contract_meta SET status='inactive', updated_at=?
        WHERE status IN ('active','observing') AND symbol NOT IN ({})""".format(
        ",".join("?" * len(onboard))), [now_ms] + list(onboard.keys()))
    conn.commit()


# ── 采集 ──────────────────────────────────

def fetch_tickers() -> list:
    r = requests.get(f"{FAPI}/fapi/v1/ticker/24hr", timeout=30)
    r.raise_for_status()
    return r.json()


def validate_and_clean(sym, data, last_price, is_observing):
    """F5: active 币跳变>50% 不入库；连续 3 次强制入库；observing 期跳过 F5"""
    price = float(data.get("lastPrice") or 0)
    vol = float(data.get("quoteVolume") or 0)
    chg = float(data.get("priceChangePercent") or 0)
    if price <= 0:
        return None
    if last_price and last_price > 0 and not is_observing:
        ratio = abs(price - last_price) / last_price
        if ratio > JUMP_RATIO:
            _jump_count[sym] = _jump_count.get(sym, 0) + 1
            if _jump_count[sym] >= JUMP_FORCE:
                _jump_count[sym] = 0
                logger.warning("[ANOMALY] %s 连续%d次跳变 %.4g->%.4g 强制入库",
                               sym, JUMP_FORCE, last_price, price)
            else:
                logger.warning("[ANOMALY] %s 价格跳变 %.4g->%.4g 跳过", sym, last_price, price)
                return None
    else:
        _jump_count[sym] = 0
    return {"price": price, "vol": vol, "chg": chg}


def persist_tickers(conn, tickers, ts):
    last_px = {r[0]: r[1] for r in conn.execute("SELECT symbol, price FROM contracts_track")}
    status = {r[0]: r[1] for r in conn.execute("SELECT symbol, status FROM contract_meta")}
    for t in tickers:
        sym = t["symbol"]
        st = status.get(sym)
        if not st or st == "inactive":
            continue
        c = validate_and_clean(sym, t, last_px.get(sym), st == "observing")
        if not c:
            continue
        conn.execute("""INSERT INTO contracts_track (symbol, price, price_change_24h, quote_volume, ts)
            VALUES (?,?,?,?,?)
            ON CONFLICT(symbol) DO UPDATE SET price=excluded.price,
                price_change_24h=excluded.price_change_24h, quote_volume=excluded.quote_volume, ts=excluded.ts""",
            (sym, c["price"], c["chg"], c["vol"], ts))
        conn.execute("""INSERT OR IGNORE INTO contracts_history (symbol, ts, price, volume_24h, price_change_24h)
            VALUES (?,?,?,?,?)""", (sym, ts, c["price"], c["vol"], c["chg"]))
    conn.commit()


def fetch_oi(symbols) -> dict:
    """10 worker + 全局节流 30req/s + 整轮 120s 硬超时；404→inactive"""
    results, sess = {}, requests.Session()

    def one(sym):
        try:
            _throttle()
            r = sess.get(f"{FAPI}/fapi/v1/openInterest", params={"symbol": sym}, timeout=10)
            if r.status_code == 404:
                return sym, None, "gone"
            r.raise_for_status()
            return sym, float(r.json()["openInterest"]), None
        except Exception as e:
            return sym, None, str(e)

    with ThreadPoolExecutor(max_workers=OI_WORKERS) as ex:
        futs = {ex.submit(one, s): s for s in symbols}
        for f in as_completed(futs, timeout=OI_TIMEOUT):
            sym, val, err = f.result()
            if err == "gone":
                results[sym] = None
            elif val is not None:
                results[sym] = val
            else:
                logger.warning("[OI] %s: %s", sym, err)
    return results


def persist_oi(conn, oi, ts):
    """OI 写独立 15min 表；404 → inactive"""
    for sym, val in oi.items():
        if val is None:
            conn.execute("UPDATE contract_meta SET status='inactive', updated_at=? WHERE symbol=?",
                         (ts, sym))
            logger.info("[INACTIVE] %s (OI 404)", sym)
            continue
        conn.execute("INSERT OR IGNORE INTO open_interest (symbol, ts, oi) VALUES (?,?,?)",
                     (sym, ts, val))
        conn.execute("UPDATE contracts_track SET open_interest=?, ts=? WHERE symbol=?",
                     (val, ts, sym))
        conn.execute("UPDATE contract_meta SET last_oi_ts=? WHERE symbol=?", (ts, sym))
    conn.commit()


def fetch_funding_all() -> dict:
    """premiumIndex 全量（ticker 没有 fundingRate 字段）"""
    r = requests.get(f"{FAPI}/fapi/v1/premiumIndex", timeout=30)
    r.raise_for_status()
    return {d["symbol"]: float(d.get("lastFundingRate") or 0) for d in r.json()}


def persist_funding(conn, funding, ts):
    for sym, rate in funding.items():
        conn.execute("INSERT OR IGNORE INTO funding_history (symbol, ts, funding_rate) VALUES (?,?,?)",
                     (sym, ts, rate))
        conn.execute("UPDATE contracts_track SET funding_rate=? WHERE symbol=?", (rate, sym))
    conn.commit()


LSR_URL = "https://fapi.binance.com/futures/data/globalLongShortAccountRatio"


def fetch_lsr_batch(symbols) -> dict:
    """多空比批量采集（globalLongShortAccountRatio, period=15m）。
    与 OI 同节奏: 并发 worker + 全局节流；404/无数据 → 跳过（该币无 LSR 属正常）"""
    results, sess = {}, requests.Session()

    def one(sym):
        try:
            _throttle()
            r = sess.get(LSR_URL, params={"symbol": sym, "period": "15m", "limit": 1}, timeout=10)
            if r.status_code == 404:
                return sym, None
            r.raise_for_status()
            data = r.json()
            if data and isinstance(data, list):
                return sym, float(data[0].get("longShortRatio"))
            return sym, None
        except Exception as e:
            logger.warning("[LSR] %s: %s", sym, e)
            return sym, None

    with ThreadPoolExecutor(max_workers=OI_WORKERS) as ex:
        futs = {ex.submit(one, s): s for s in symbols}
        for f in as_completed(futs, timeout=OI_TIMEOUT):
            sym, val = f.result()
            if val is not None:
                results[sym] = val
    return results


def persist_lsr(conn, lsr, ts):
    """LSR 写独立 15min 表 + 回填 contracts_track.lsr"""
    for sym, val in lsr.items():
        conn.execute("INSERT OR IGNORE INTO long_short_ratio (symbol, ts, lsr) VALUES (?,?,?)",
                     (sym, ts, val))
        conn.execute("UPDATE contracts_track SET lsr=? WHERE symbol=?", (val, sym))
    conn.commit()


def fetch_depth() -> list:
    """Top50（按成交额）深度，limit=20 档聚合买卖盘金额"""
    conn = connect()
    tops = conn.execute("""SELECT symbol FROM contracts_track
        WHERE open_interest > 0 ORDER BY quote_volume DESC LIMIT ?""", (DEPTH_TOP,)).fetchall()
    conn.close()
    out, sess = [], requests.Session()
    for (sym,) in tops:
        try:
            _throttle()
            r = sess.get(f"{FAPI}/fapi/v1/depth",
                         params={"symbol": sym, "limit": DEPTH_LIMIT}, timeout=10)
            r.raise_for_status()
            d = r.json()
            bid = sum(float(p) * float(q) for p, q in d.get("bids", []))
            ask = sum(float(p) * float(q) for p, q in d.get("asks", []))
            spread = (float(d["asks"][0][0]) - float(d["bids"][0][0])) / float(d["bids"][0][0]) \
                     if d.get("bids") and d.get("asks") else None
            out.append((sym, bid, ask, spread))
        except Exception as e:
            logger.warning("[DEPTH] %s: %s", sym, e)
    return out


def archive(conn, now_ms):
    """热 7 天→1h 降采样；OI/Funding 保留 30 天；VACUUM 失败仅告警"""
    hot = now_ms - 7 * 86400000
    conn.execute("""INSERT OR IGNORE INTO contracts_history_hourly
        SELECT symbol, (ts/3600000)*3600000, AVG(price), MAX(volume_24h), AVG(price_change_24h)
        FROM contracts_history WHERE ts < ? GROUP BY symbol, (ts/3600000)""", (hot,))
    conn.execute("DELETE FROM contracts_history WHERE ts < ?", (hot,))
    conn.execute("DELETE FROM open_interest WHERE ts < ?", (now_ms - 30 * 86400000,))
    conn.execute("DELETE FROM funding_history WHERE ts < ?", (now_ms - 30 * 86400000,))
    conn.execute("DELETE FROM long_short_ratio WHERE ts < ?", (now_ms - 30 * 86400000,))
    conn.commit()
    try:
        conn.execute("VACUUM")
    except Exception as e:
        logger.warning("VACUUM skip (WAL 下可稍后重试): %s", e)


# ── 主循环 ──────────────────────────────────

def main_loop():
    init_db()
    cycle = 0
    while True:
        t0, ts = time.time(), int(time.time() * 1000)
        try:
            tickers = fetch_tickers()
            conn = connect()
            discover_new(conn, tickers, ts)
            persist_tickers(conn, tickers, ts)
            conn.close()

            if cycle % OI_EVERY == 0:                     # 15min
                conn = connect()
                syms = [r[0] for r in conn.execute(
                    "SELECT symbol FROM contract_meta WHERE status IN ('active','observing')")]
                persist_oi(conn, fetch_oi(syms), ts)
                persist_lsr(conn, fetch_lsr_batch(syms), ts)   # LSR 与 OI 同节奏
                promote_observing(conn, ts)               # 15min 检查转正（含 R1 提前转正）
                conn.close()

            now = datetime.now(timezone.utc)
            if now.hour in (23, 7, 15) and now.minute >= 55:   # 结算前 5min 拉费率
                conn = connect()
                persist_funding(conn, fetch_funding_all(), ts)
                conn.close()

            if cycle % DEPTH_EVERY == 0:                  # 1h
                conn = connect()
                for row in fetch_depth():
                    conn.execute("INSERT OR IGNORE INTO contracts_depth VALUES (?,?,?,?,?)",
                                 (row[0], ts) + row[1:])
                conn.commit(); conn.close()

            if now.hour == 0 and 0 <= now.minute <= 5:    # 每日 00:00-00:05 同步+归档
                conn = connect()
                sync_contracts(conn, ts)
                archive(conn, ts)
                conn.close()

            logger.info(json.dumps({"event": "tick", "ts": ts,
                                    "ticker_count": len(tickers),
                                    "duration_ms": int((time.time() - t0) * 1000)}))
        except Exception as e:
            logger.error("cycle error: %s", e)
        cycle += 1
        time.sleep(max(0, CYCLE - (time.time() - t0)))


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    main_loop()
```

---

## 四、信号层 signal_engine.py（完整代码 v2.2）

```python
#!/usr/bin/env python3
"""P2 信号引擎 v2.2 — 每小时整点跑一次，跑完 sys.exit(0)（配合 PM2 cron_restart）"""
import json, logging, sqlite3, sys, time

DB_PATH = "/root/workspace/analysis_notes/market_radar.db"

# R2: 命中阈值按信号类型差异化（确认项 3）
HIT_THRESHOLD = {
    "price_surge":             0.02,
    "oi_spike":                0.015,
    "funding_flip":            0.01,
    "strong_momentum":         0.03,
    "new_listing":             0.05,
    "volume_price_divergence": 0.015,
    "sustained_momentum":      0.02,
}

# 全部阈值集中（阶段5调参只改这里）
SIGNAL_DEFS = {
    "price_surge":             {"threshold": 10.0, "weight": 15, "dedup_h": 1,  "dir": "long"},
    "oi_spike":                {"threshold": 0.15, "weight": 20, "dedup_h": 1,  "dir": "long"},
    "funding_flip":            {"threshold": 0.0,  "weight": 12, "dedup_h": 8,  "dir": "long"},
    "sustained_momentum":      {"threshold": 3,    "weight": 18, "dedup_h": 1,  "dir": "long"},
    "new_listing":             {"threshold": 24,   "weight": 25, "dedup_h": 24, "dir": "neutral"},
    "volume_price_divergence": {"threshold": 0.0,  "weight": 8,  "dedup_h": 4,  "dir": None},
    "strong_momentum":         {"threshold": 0.0,  "weight": 30, "dedup_h": 1,  "dir": "long"},
}

logger = logging.getLogger("p2-signal")


def connect():
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.execute("PRAGMA busy_timeout=30000")
    return conn


def signal_direction(sig_type, params):
    """shrink_rise=缩量上涨(衰竭→看空)，expand_fall=放量下跌(吸筹→看多)"""
    if sig_type == "volume_price_divergence":
        return "short" if params.get("type") == "shrink_rise" else "long"
    return SIGNAL_DEFS[sig_type]["dir"]


def emit(conn, sym, sig_type, strength, params, ts) -> bool:
    """应用层去重 + expires=max(dedup,4)h + 快照 + 性能表初始化"""
    dedup_h = SIGNAL_DEFS[sig_type]["dedup_h"]
    if conn.execute("SELECT 1 FROM signals WHERE symbol=? AND signal_type=? AND ts>? LIMIT 1",
                    (sym, sig_type, ts - dedup_h * 3600000)).fetchone():
        return False
    expires = ts + max(dedup_h, 4) * 3600000
    cur = conn.execute("""INSERT INTO signals (symbol, signal_type, direction, strength, confidence, params, ts, expires_at)
        VALUES (?,?,?,?,?,?,?,?)""",
        (sym, sig_type, signal_direction(sig_type, params), strength, 0.7,
         json.dumps(params), ts, expires))
    tr = conn.execute("""SELECT price, open_interest, quote_volume, funding_rate, price_change_24h
        FROM contracts_track WHERE symbol=?""", (sym,)).fetchone()
    if tr:
        conn.execute("INSERT INTO signal_snapshots VALUES (?,?,?,?,?,?,?,?)",
                     (cur.lastrowid, sym, tr[0], tr[1], tr[2], tr[3], tr[4], ts))
        conn.execute("""INSERT INTO signal_performance (signal_id, symbol, signal_type, direction, entry_price)
            VALUES (?,?,?,?,?)""", (cur.lastrowid, sym, sig_type,
                                    signal_direction(sig_type, params), tr[0]))
    return True


def backfill_performance(conn, now):
    """对 1h/4h/24h 前信号回填 exit_price/pnl/hit（R2: 按类型取阈值）"""
    for label, hours in (("1h", 1), ("4h", 4), ("24h", 24)):
        rows = conn.execute(f"""SELECT sp.signal_id, sp.symbol, sp.signal_type, sp.direction, sp.entry_price
            FROM signal_performance sp JOIN signals s ON s.id = sp.signal_id
            WHERE s.ts <= ? AND sp.exit_price_{label} IS NULL""",
            (now - hours * 3600000,)).fetchall()
        for sid, sym, sig_type, direction, entry in rows:
            if not entry:
                continue
            row = conn.execute("SELECT price FROM contracts_track WHERE symbol=?", (sym,)).fetchone()
            if not row:
                continue
            pnl = (row[0] - entry) / entry * (1 if direction == "long" else -1)
            hit = 1 if pnl >= HIT_THRESHOLD.get(sig_type, 0.02) else 0
            conn.execute(f"""UPDATE signal_performance SET exit_price_{label}=?,
                pnl_{label}=?, hit_{label}=?, evaluated_at=? WHERE signal_id=?""",
                (row[0], round(pnl, 6), hit, now, sid))
    conn.commit()


def run():
    conn = connect()
    ts = int(time.time() * 1000)
    window = 24 * 3600000
    backfill_performance(conn, ts)

    # 一次 SQL 全量拉取，内存分组（1360 次查询 → 3 条）
    hist, oi, fund = {}, {}, {}
    for sym, t, p, v, c in conn.execute(
            "SELECT symbol, ts, price, volume_24h, price_change_24h FROM contracts_history WHERE ts>? ORDER BY symbol, ts",
            (ts - window,)):
        hist.setdefault(sym, []).append({"ts": t, "price": p, "vol": v, "chg": c})
    for sym, t, o in conn.execute(
            "SELECT symbol, ts, oi FROM open_interest WHERE ts>? ORDER BY symbol, ts", (ts - window,)):
        oi.setdefault(sym, []).append((t, o))
    for sym, t, f in conn.execute(
            "SELECT symbol, ts, funding_rate FROM funding_history WHERE ts>? ORDER BY symbol, ts",
            (ts - window,)):
        fund.setdefault(sym, []).append((t, f))
    new_listings = {r[0]: r[1] for r in conn.execute(
        "SELECT symbol, listing_time FROM contract_meta WHERE listing_time > ?", (ts - 86400000,))}

    # 每整点涨幅排名（sustained_momentum 用）
    hour_chg = {}
    for sym, arr in hist.items():
        for r in arr:
            h = r["ts"] // 3600000 * 3600000
            hour_chg.setdefault(h, {})[sym] = r["chg"]
    top50 = {h: set(s for s, _ in sorted(d.items(), key=lambda kv: kv[1] or -999, reverse=True)[:50])
             for h, d in hour_chg.items()}
    recent_hours = sorted(hour_chg)[-3:]

    summary = {k: 0 for k in SIGNAL_DEFS}

    # R5: 对 active + observing 全量算信号（状态机需要；消费层只认 active）
    for sym, arr in hist.items():
        if len(arr) < 12:
            continue
        latest, prev_1h, prev_4h = arr[-1], arr[-12], arr[-48] if len(arr) >= 48 else None

        # 1) price_surge: 24h 涨幅 >10%
        if latest["chg"] and latest["chg"] > SIGNAL_DEFS["price_surge"]["threshold"]:
            if emit(conn, sym, "price_surge", min(int(latest["chg"] * 5), 100),
                    {"change_24h": latest["chg"]}, ts):
                summary["price_surge"] += 1

        # 2) oi_spike: OI 1h 变化 >15%（独立表，不受 NULL 稀疏影响）
        oi_arr = oi.get(sym, [])
        if len(oi_arr) >= 2:
            oi_now = oi_arr[-1][1]
            oi_prev1h = oi_arr[-4][1] if len(oi_arr) >= 4 else oi_arr[0][1]
            if oi_prev1h and oi_now:
                oi_chg = (oi_now - oi_prev1h) / oi_prev1h
                if oi_chg > SIGNAL_DEFS["oi_spike"]["threshold"]:
                    if emit(conn, sym, "oi_spike", min(int(oi_chg * 600), 100),
                            {"oi_change": round(oi_chg, 4)}, ts):
                        summary["oi_spike"] += 1

        # 3) funding_flip: 最近两期费率从负转正
        fund_arr = fund.get(sym, [])
        if len(fund_arr) >= 2:
            f_prev, f_now = fund_arr[-2][1], fund_arr[-1][1]
            if f_prev < 0 and f_now >= 0:
                if emit(conn, sym, "funding_flip", 70,
                        {"from": f_prev, "to": f_now}, ts):
                    summary["funding_flip"] += 1

        # 4) sustained_momentum: 连续 3 个整点在涨幅榜前 50
        if len(recent_hours) == 3 and all(sym in top50.get(h, set()) for h in recent_hours):
            if emit(conn, sym, "sustained_momentum", 75, {"hours": 3}, ts):
                summary["sustained_momentum"] += 1

        # 5) volume_price_divergence: F4 区间成交量
        if prev_4h and latest["price"] and prev_4h["price"]:
            p_chg = (latest["price"] - prev_4h["price"]) / prev_4h["price"]
            v_4h = interval_volume(arr[-48:])
            v_prev = interval_volume(arr[-96:-48]) if len(arr) >= 96 else v_4h
            v_chg = (v_4h - v_prev) / v_prev if v_prev else 0
            if p_chg > 0.03 and v_chg < -0.10:          # 缩量上涨 → 衰竭，看空
                if emit(conn, sym, "volume_price_divergence", min(int(abs(v_chg) * 400), 100),
                        {"type": "shrink_rise", "p": round(p_chg, 4), "v": round(v_chg, 4)}, ts):
                    summary["volume_price_divergence"] += 1
            if p_chg < -0.03 and v_chg > 0.20:          # 放量下跌 → 吸筹，看多
                if emit(conn, sym, "volume_price_divergence", min(int(v_chg * 400), 100),
                        {"type": "expand_fall", "p": round(p_chg, 4), "v": round(v_chg, 4)}, ts):
                    summary["volume_price_divergence"] += 1

    # 6) new_listing: 上市 <24h
    for sym, listing in new_listings.items():
        if emit(conn, sym, "new_listing", 80, {"listing_time": listing}, ts):
            summary["new_listing"] += 1

    # 7) strong_momentum: 1h 内 price_surge + oi_spike 同现（F6: 独立权重不叠加）
    rows = conn.execute("""SELECT symbol FROM signals
        WHERE ts > ? AND signal_type IN ('price_surge','oi_spike') GROUP BY symbol
        HAVING COUNT(DISTINCT signal_type) = 2""", (ts - 3600000,)).fetchall()
    for (sym,) in rows:
        if emit(conn, sym, "strong_momentum", 90, {"components": ["price_surge", "oi_spike"]}, ts):
            summary["strong_momentum"] += 1

    # F7 监控
    total = sum(summary.values())
    print(json.dumps({"event": "signal_summary", "ts": ts, "total": total, "by_type": summary}))
    conn.commit(); conn.close()
    sys.exit(0)     # R3: 跑完即退，PM2 到整点再拉起


def interval_volume(arr) -> float:
    """F4: 窗口内滚动成交额 max-min ≈ 区间增量"""
    vols = [r["vol"] for r in arr if r["vol"]]
    return (max(vols) - min(vols)) if len(vols) >= 2 else 0.0


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    run()
```

---

## 五、消费层

### 5.1 Screener 加分（F6 + R5 过滤）

```python
def score_with_p2_signals(symbol: str, base_score: int) -> int:
    """读 signals 加分。F6: strong_momentum 独立 +40 不叠加子信号。
    R5: 只认 active 币的信号（observing 币不进交易链路评分）"""
    conn = sqlite3.connect(DB_PATH, timeout=30)
    ts = int(time.time() * 1000)
    rows = conn.execute("""
        SELECT s.signal_type FROM signals s
        JOIN contract_meta m ON m.symbol = s.symbol AND m.status = 'active'
        WHERE s.symbol=? AND s.ts > ? AND s.expires_at > ?
    """, (symbol, ts - 4*3600*1000, ts)).fetchall()
    types = {r[0] for r in rows}
    bonus = 0
    if 'strong_momentum' in types:
        bonus = 40
    else:
        if 'oi_spike' in types: bonus += 15
        if 'price_surge' in types: bonus += 10
        if 'funding_flip' in types: bonus += 8
        if 'volume_price_divergence' in types: bonus += 5
    conn.close()
    return base_score + min(bonus, 50)
```

### 5.2 algochip.ai 路由（token_radar/dashboard.py 追加）

```
GET /api/p2/track        → contracts_track ORDER BY price_change_24h DESC（涨幅榜）
GET /api/p2/signals      → 未过期信号（?signal_type=&limit=）
GET /api/p2/performance  → signal_performance 命中率统计（阶段 4/5 复盘）
新 Tab「持续跟踪」：实时涨幅榜 / OI 异动 / 费率监控 / 量价背离 / 强势信号 / 新币
```

### 5.3 健康检查 p2_healthcheck.py（cron 每 10min，R4: 单行 JSON）

```python
#!/usr/bin/env python3
"""P2 健康检查 — 输出单行 JSON，便于 jq 与后续接通道"""
import json, sqlite3, time

DB_PATH = "/root/workspace/analysis_notes/market_radar.db"

def main():
    conn = sqlite3.connect(DB_PATH, timeout=10)
    now_ms = int(time.time() * 1000)
    errors = []

    last_hist = conn.execute("SELECT MAX(ts) FROM contracts_history").fetchone()[0]
    if not last_hist or now_ms - last_hist > 10 * 60 * 1000:
        errors.append("data_stale")

    last_oi = conn.execute("SELECT MAX(ts) FROM open_interest").fetchone()[0]
    if not last_oi or now_ms - last_oi > 25 * 60 * 1000:
        errors.append("oi_stale")

    last_sig = conn.execute("SELECT MAX(ts) FROM signals").fetchone()[0]
    if not last_sig or now_ms - last_sig > 95 * 60 * 1000:
        errors.append("signal_stale")

    sig_24h = conn.execute(
        "SELECT COUNT(*) FROM signals WHERE ts > ?", (now_ms - 24*3600*1000,)).fetchone()[0]
    if sig_24h == 0:
        errors.append("signal_empty_24h")

    conn.close()
    out = {"event": "healthcheck", "ts": now_ms,
           "status": "ok" if not errors else "critical",
           "errors": errors,
           "checks": {"last_history_ms": last_hist, "last_oi_ms": last_oi,
                      "last_signal_ms": last_sig, "signals_24h": sig_24h}}
    print(json.dumps(out))

if __name__ == "__main__":
    main()
```

---

## 六、运维与监控

### 6.1 PM2 配置（ecosystem.config.js 追加）

```js
{
  name: 'p2-tracker',
  script: '/root/workspace/analysis_notes/contracts_tracker.py',
  interpreter: '/root/irisquant/venv/bin/python3',
  instances: 1,
  exec_mode: 'fork',
  autorestart: true,
  max_restarts: 10,
  min_uptime: '10s',
  log_date_format: 'YYYY-MM-DD HH:mm:ss Z',
  cron_restart: '0 2 * * *',      // 每日 2 点重启兜底
  env: { PYTHONPATH: '/root/workspace/analysis_notes' }
},
{
  name: 'p2-signal',
  script: '/root/workspace/analysis_notes/signal_engine.py',
  interpreter: '/root/irisquant/venv/bin/python3',
  instances: 1,
  exec_mode: 'fork',
  autorestart: false,              // R3: 跑完即退，不自动重启
  cron_restart: '0 * * * *',       // 每小时整点拉起
  env: { PYTHONPATH: '/root/workspace/analysis_notes' }
},
{
  name: 'p2-healthcheck',
  script: '/root/workspace/analysis_notes/p2_healthcheck.py',
  interpreter: '/root/irisquant/venv/bin/python3',
  instances: 1,
  exec_mode: 'fork',
  autorestart: false,
  cron_restart: '*/10 * * * *',    // 每 10 分钟
  env: { PYTHONPATH: '/root/workspace/analysis_notes' }
}
```

### 6.2 监控指标与告警

```
tracker:  {"event":"tick","ticker_count":679,"duration_ms":4500}
signal:   {"event":"signal_summary","total":35,"by_type":{...}}（F7）
health:   {"event":"healthcheck","status":"ok|critical","errors":[...]}（R4）

告警触发（healthcheck 输出 critical 即触发）:
  data_stale       history 最新 < now-10min
  oi_stale         OI 最新 < now-25min
  signal_stale     信号最新 < now-95min
  signal_empty_24h 24h 内信号总数为 0
  另: 日志 [OI] 429 计数上升 → 限流（人工观察，暂不自动化）
```

---

## 七、落地路线图（4 阶段）

| 阶段 | 时间 | 任务 | 验收标准 |
|------|------|------|---------|
| 1 | 第 1 周 | 清库重建(10表) + tracker + 状态机 + WAL + PM2 | 24h 稳定；DB 每 5min 新数据；OI 无 429；存量 679 币全部 active（无死锁）；新币 5min 内进 observing |
| 2 | 第 2 周 | signal_engine 7 信号 + F4 + F7 + 性能回填 + healthcheck | signals 每小时有产出；7 类信号至少 5 类触发过；performance 1h 列正常回填 |
| 3 | 第 3 周 | Screener 接入(F6+R5) + algochip.ai Tab + API | KOMA 类币 strong_momentum 高分；Dashboard 延迟 <15min |
| 4 | 第 4 周 | 复盘闭环：命中率分析 + 阈值调参 + 突破前蓄力观察 | 信号胜率 >40%，假信号 <30%；据此调 SIGNAL_DEFS/常量区 |

---

## 八、关键决策确认（6 项全部确认 ✅）

| 序号 | 事项 | 结论 |
|------|------|------|
| 1 | 旧库清理重建 | ✅ 备份后重建（mv 到 backups/ + 删 tracker_state.json） |
| 2 | 转正门槛 48h+10点+2M | ✅ 接受，增加 R1 提前转正（strong_momentum 触发即转正） |
| 3 | 命中定义 ≥2% | ✅ 接受初值，R2 按信号类型差异化配置 |
| 4 | 调度方式 | ✅ PM2 cron_restart + R3 主动退出 |
| 5 | 告警通道 | ✅ 先日志 + 单行 JSON，TG 可选 |
| 6 | 方案定稿 | ✅ 定稿 v2.2，进入阶段 1 |

---

## 九、阶段 1 开工清单（等用户最终确认后执行）

1. 备份旧库：mv market_radar.db → backups/market_radar.db.bak.$(date +%Y%m%d)；rm tracker_state.json
2. 写 contracts_tracker.py（第三节代码全文）+ signal_engine.py + p2_healthcheck.py
3. python 语法检查（py_compile）
4. 手动跑一轮 init_db + sync_contracts + 单次 ticker 采集验证落库
5. 验证存量币全部 active（无转正死锁）
6. PM2 注册 3 个进程并启动
7. 验证：DB 每 5min 新数据 / OI 无 429 / healthcheck ok
8. 汇报验证结果给用户
