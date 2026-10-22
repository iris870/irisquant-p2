#!/usr/bin/env python3
"""P2 全合约持续跟踪 — 采集层 v2.2（Daemon）
频率: Ticker 5min | OI 15min(全局30req/s) | Funding 8h | 深度 1h | 合约同步 每日+5min实时发现
"""
import json
import logging
import sqlite3
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

import requests

DB_PATH = "/root/workspace/analysis_notes/market_radar.db"   # 绝对路径
FAPI = "https://fapi.binance.com"

# ── 集中常量区（阶段5调参只改这里）──
CYCLE = 300            # ticker 5min
OI_EVERY = 3           # OI 每 3 周期 = 15min
DEPTH_EVERY = 12       # 深度每 12 周期 = 1h
OBSERVE_HOURS = 48     # 新币观察期
PROMOTE_OI_PTS = 10    # 转正: 24h 内 OI 采样点 ≥10
PROMOTE_VOL = 2_000_000  # 转正: 24h 成交额 ≥ 2M USDT
JUMP_RATIO = 0.5       # F5 跳变阈值 50%
JUMP_FORCE = 3         # 连续 3 次跳变强制入库
OI_RPS = 30            # 全局限流 30 req/s（Binance 上限 ~40）
OI_WORKERS = 10
OI_TIMEOUT = 120       # 整轮硬超时
DEPTH_TOP = 50
DEPTH_LIMIT = 20       # 深度档位

logger = logging.getLogger("p2-tracker")
_jump_count = {}                            # F5 逃生口状态
_throttle_lock, _throttle_last = threading.Lock(), [0.0]
_exchange_cache = {"ts": 0, "data": {}}

SCHEMA = """
PRAGMA journal_mode=WAL;
PRAGMA synchronous=NORMAL;

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

-- 7b. 大户持仓比（15min，2026-10-12 批次B 新增）
--     topLongShortPositionRatio：大户真金白银的多空方向（仓位口径）
CREATE TABLE IF NOT EXISTS top_position_ratio (
    symbol          TEXT NOT NULL,
    ts              INTEGER NOT NULL,
    long_account    REAL,      -- 多头账户占比 0-1
    short_account   REAL,
    ls_ratio        REAL,      -- long/short
    UNIQUE(symbol, ts)
);
CREATE INDEX IF NOT EXISTS idx_tpos_symbol_ts ON top_position_ratio(symbol, ts);

-- 7c. 大户账户比（15min，2026-10-12 批次B 新增）
--     topLongShortAccountRatio：大户人数口径（与持仓比背离 = 少数大仓 vs 多数小仓）
CREATE TABLE IF NOT EXISTS top_account_ratio (
    symbol          TEXT NOT NULL,
    ts              INTEGER NOT NULL,
    long_account    REAL,
    short_account   REAL,
    ls_ratio        REAL,
    UNIQUE(symbol, ts)
);
CREATE INDEX IF NOT EXISTS idx_tacc_symbol_ts ON top_account_ratio(symbol, ts);

-- 7d. 主动买卖量比（15min，2026-10-12 批次B 新增）
--     takerlongshortRatio：taker 主动买/卖，真实买卖压（不是挂单口径）
CREATE TABLE IF NOT EXISTS taker_ratio (
    symbol          TEXT NOT NULL,
    ts              INTEGER NOT NULL,
    buy_sell_ratio  REAL,
    buy_vol         REAL,
    sell_vol        REAL,
    UNIQUE(symbol, ts)
);
CREATE INDEX IF NOT EXISTS idx_taker_symbol_ts ON taker_ratio(symbol, ts);

-- ── 7e. 2026-10-12 批次D：30 天到期前的 1h 降采样归档表 ──
--     原先 open_interest / long_short_ratio 到 30 天直接 DELETE 无降采样
--     （每 30 天永久丢 227 万行；9/12 之前的数据已丢）。现在先降采样再删。
CREATE TABLE IF NOT EXISTS open_interest_hourly (
    symbol          TEXT NOT NULL,
    ts              INTEGER NOT NULL,
    oi_avg          REAL,
    oi_max          REAL,
    oi_min          REAL,
    PRIMARY KEY (symbol, ts)
);
CREATE INDEX IF NOT EXISTS idx_oi_hourly_ts ON open_interest_hourly(ts);

CREATE TABLE IF NOT EXISTS long_short_ratio_hourly (
    symbol          TEXT NOT NULL,
    ts              INTEGER NOT NULL,
    lsr_avg         REAL,
    lsr_max         REAL,
    lsr_min         REAL,
    PRIMARY KEY (symbol, ts)
);
CREATE INDEX IF NOT EXISTS idx_lsr_hourly_ts ON long_short_ratio_hourly(ts);

-- ── 2026-10-12 R3：三类新数据的 1h 降采样归档表（30 天热数据到期前的保底）──
CREATE TABLE IF NOT EXISTS top_position_ratio_hourly (
    symbol          TEXT NOT NULL,
    ts              INTEGER NOT NULL,
    lsr_avg         REAL,
    lsr_max         REAL,
    lsr_min         REAL,
    PRIMARY KEY (symbol, ts)
);
CREATE INDEX IF NOT EXISTS idx_tpos_hourly_ts ON top_position_ratio_hourly(ts);

CREATE TABLE IF NOT EXISTS top_account_ratio_hourly (
    symbol          TEXT NOT NULL,
    ts              INTEGER NOT NULL,
    lsr_avg         REAL,
    lsr_max         REAL,
    lsr_min         REAL,
    PRIMARY KEY (symbol, ts)
);
CREATE INDEX IF NOT EXISTS idx_tacc_hourly_ts ON top_account_ratio_hourly(ts);

CREATE TABLE IF NOT EXISTS taker_ratio_hourly (
    symbol          TEXT NOT NULL,
    ts              INTEGER NOT NULL,
    bs_avg          REAL,
    bs_max          REAL,
    bs_min          REAL,
    PRIMARY KEY (symbol, ts)
);
CREATE INDEX IF NOT EXISTS idx_taker_hourly_ts ON taker_ratio_hourly(ts);

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
"""


def connect() -> sqlite3.Connection:
    """每个连接都设 busy_timeout + synchronous=NORMAL（防 database is locked）"""
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.execute("PRAGMA busy_timeout=30000")
    conn.execute("PRAGMA synchronous=NORMAL")
    return conn


def init_db():
    """建全部表（幂等），部署即用；已有库做 ALTER 兼容迁移"""
    conn = connect()
    conn.execute("PRAGMA journal_mode=WAL")
    conn.executescript(SCHEMA)
    # 兼容迁移: 老库 contracts_track 无 lsr 列时补上
    cols = [r[1] for r in conn.execute("PRAGMA table_info(contracts_track)")]
    if "lsr" not in cols:
        conn.execute("ALTER TABLE contracts_track ADD COLUMN lsr REAL")
    # 2026-10-12 批次B: contract_meta 加 funding_interval / last_funding_ts
    ensure_funding_interval_col(conn)
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

def fetch_onboard_dates(force: bool = False) -> dict:
    """exchangeInfo 全量缓存 1h，返回 {symbol: onboardDate_ms}

    force=True 时绕过缓存强制刷新 —— 用于新币首次发现时补 onboardDate
    （1h 缓存可能先于该币上市被抓取，导致 onboard.get(sym) 为 None）。
    """
    now = time.time()
    if force or now - _exchange_cache["ts"] > 3600:
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
    refreshed = False
    for t in tickers:
        sym = t["symbol"]
        if sym in known:
            continue
        listing = onboard.get(sym)
        # 2026-10-21 修复：新币首次发现时 1h 缓存可能还没包含它（缓存先于上市被抓取），
        # 导致 listing_time 落 NULL → monitor_new_contracts 的 24h~72h 判方向窗口永远漏掉该币。
        # 先强制刷新一次 exchangeInfo 补 onboardDate；仍取不到则用 now_ms 兜底（新币 listing≈now）。
        if listing is None and not refreshed:
            onboard = fetch_onboard_dates(force=True)
            refreshed = True
            listing = onboard.get(sym)
        if listing is None:
            listing = now_ms
            logger.warning("[NEW] %s onboardDate 缺失，listing_time 兜底=now_ms", sym)
        conn.execute("""INSERT OR IGNORE INTO contract_meta
            (symbol, status, observing_since, listing_time, updated_at)
            VALUES (?, 'observing', ?, ?, ?)""",
                     (sym, now_ms, listing, now_ms))
        logger.info("[NEW] %s -> observing (listing=%s)", sym, listing)
    conn.commit()


def promote_observing(conn, now_ms):
    """转正通道：
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


# ── 2026-10-12 批次B：三类新增合约数据（官方仅 30 天窗口，越晚补越丢）──

TOP_POS_URL = "https://fapi.binance.com/futures/data/topLongShortPositionRatio"
TOP_ACC_URL = "https://fapi.binance.com/futures/data/topLongShortAccountRatio"
TAKER_URL = "https://fapi.binance.com/futures/data/takerlongshortRatio"

# 新增 3 端点采集池上限（2026-10-12 P0-A 修正）。
# ⚠️ 原为 300，实测只覆盖 36.5% 的 P2 信号 symbol（740 个信号仅 270 个在 Top300），
#    而回测结论（保留率 3-13%）建立在全池 804 币截面上 → 池不足会让下游过滤误拒。
#    池规模扫描：200→24.2% / 300→36.5% / 500→62.8% / 700→88.8% / 804→100%。
#    实测限流余量：连打 60 次全 200（4.9s）；全池 804×3=2412 req @30req/s ≈ 81s，
#    p2-tracker 单轮实测 94.9s 可容纳（批量硬超时 OI_TIMEOUT=120s）。
RATIO_TOP = 900          # ≥ 活跃池（约 806），留余量

# 比值类字段合理值域：越界视为脏值（longAccount=0 时 longShortRatio 会发散成 0/Inf）
RATIO_LO, RATIO_HI = 0.01, 100.0


def _fetch_ratio_batch(url, symbols, fields):
    """通用：批量拉 futures/data/*Ratio（period=15m, limit=1）

    fields: 从响应条目提取的字段名元组，按顺序返回 tuple
    返回 {symbol: tuple(...)}；404/无数据 → 跳过（该币无此数据属正常）
    """
    results, sess = {}, requests.Session()

    def one(sym):
        try:
            _throttle()
            r = sess.get(url, params={"symbol": sym, "period": "15m", "limit": 1}, timeout=10)
            if r.status_code == 404:
                return sym, None
            r.raise_for_status()
            data = r.json()
            if data and isinstance(data, list):
                d = data[0]
                vals = []
                for f in fields:
                    v = d.get(f)
                    if v is None:
                        vals.append(None)
                        continue
                    try:
                        fv = float(v)
                    except (TypeError, ValueError):
                        vals.append(None)
                        continue
                    # 2026-10-12 P1-1: 比值字段值域守卫。
                    #   实测 top_position_ratio 有 40 行 ls_ratio=0 或 >100 的脏值，
                    #   根因是 longAccount=0 时 longShortRatio 发散（0 或 Inf）。
                    #   裸落库会把 0/Inf 传给消费方 → 造成错误否决。越界统一落 None。
                    if f in ("longShortRatio", "buySellRatio", "longAccount",
                             "shortAccount", "buyVol", "sellVol"):
                        if f in ("longShortRatio", "buySellRatio"):
                            if not (RATIO_LO < fv < RATIO_HI):
                                vals.append(None)
                                continue
                        elif f in ("longAccount", "shortAccount"):
                            if not (0.0 <= fv <= 1.0):
                                vals.append(None)
                                continue
                        else:  # buyVol / sellVol：非负
                            if fv < 0:
                                vals.append(None)
                                continue
                    vals.append(fv)
                return sym, tuple(vals)
            return sym, None
        except Exception as e:
            logger.warning("[ratio] %s %s: %s", url.rsplit("/", 1)[-1], sym, e)
            return sym, None

    with ThreadPoolExecutor(max_workers=OI_WORKERS) as ex:
        futs = {ex.submit(one, s): s for s in symbols}
        for f in as_completed(futs, timeout=OI_TIMEOUT):
            sym, val = f.result()
            if val is not None:
                results[sym] = val
    return results


def fetch_top_position_batch(symbols) -> dict:
    """大户持仓比（topLongShortPositionRatio）"""
    return _fetch_ratio_batch(
        TOP_POS_URL, symbols, ("longAccount", "shortAccount", "longShortRatio"))


def fetch_top_account_batch(symbols) -> dict:
    """大户账户比（topLongShortAccountRatio）"""
    return _fetch_ratio_batch(
        TOP_ACC_URL, symbols, ("longAccount", "shortAccount", "longShortRatio"))


def fetch_taker_batch(symbols) -> dict:
    """主动买卖量比（takerlongshortRatio）"""
    return _fetch_ratio_batch(
        TAKER_URL, symbols, ("buySellRatio", "buyVol", "sellVol"))


def persist_top_position(conn, data, ts):
    for sym, v in data.items():
        conn.execute("INSERT OR IGNORE INTO top_position_ratio "
                     "(symbol, ts, long_account, short_account, ls_ratio) VALUES (?,?,?,?,?)",
                     (sym, ts) + v)
    conn.commit()


def persist_top_account(conn, data, ts):
    for sym, v in data.items():
        conn.execute("INSERT OR IGNORE INTO top_account_ratio "
                     "(symbol, ts, long_account, short_account, ls_ratio) VALUES (?,?,?,?,?)",
                     (sym, ts) + v)
    conn.commit()


def persist_taker(conn, data, ts):
    for sym, v in data.items():
        conn.execute("INSERT OR IGNORE INTO taker_ratio "
                     "(symbol, ts, buy_sell_ratio, buy_vol, sell_vol) VALUES (?,?,?,?,?)",
                     (sym, ts) + v)
    conn.commit()


# ── 2026-10-12 批次B：funding 按各币真实结算周期采集 ──

FUNDING_INTERVAL_FILE = "/root/workspace/analysis_notes/funding_intervals.json"


def load_funding_intervals() -> dict:
    """拉取 fundingInfo 得到 {symbol: interval_hours}，落盘缓存

    实测分布：8h=340 个 / 4h=468 个 / 1h=1 个。原先固定 3 次/天 → 468 个 4h 币隔期采样。
    """
    try:
        r = requests.get(f"{FAPI}/fapi/v1/fundingInfo", timeout=30)
        r.raise_for_status()
        d = {x["symbol"]: int(x.get("fundingIntervalHours", 8)) for x in r.json()}
        with open(FUNDING_INTERVAL_FILE, "w") as f:
            json.dump(d, f)
        return d
    except Exception as e:
        logger.warning("fundingInfo 拉取失败，改用缓存: %s", e)
        try:
            with open(FUNDING_INTERVAL_FILE) as f:
                return json.load(f)
        except Exception:
            return {}


def ensure_funding_interval_col(conn):
    """幂等迁移：contract_meta 加 funding_interval / last_funding_ts 列"""
    for col, typ in (("funding_interval", "INTEGER"), ("last_funding_ts", "INTEGER")):
        try:
            conn.execute(f"ALTER TABLE contract_meta ADD COLUMN {col} {typ}")
        except sqlite3.OperationalError as e:
            if "duplicate column" not in str(e).lower():
                logger.warning("ALTER %s: %s", col, e)
    conn.commit()


def funding_due_symbols(conn, intervals: dict, now_ms: int) -> list:
    """返回「距上次采集 ≥ 结算周期 90%」的币（按币独立判定，取代全局 3 时点）"""
    out = []
    for sym, iv, ts in conn.execute(
            "SELECT symbol, COALESCE(funding_interval, 8), last_funding_ts "
            "FROM contract_meta WHERE status IN ('active','observing')"):
        if not ts or now_ms - int(ts) >= int(iv) * 3600000 * 0.9:
            out.append(sym)
    return out


def fetch_funding_for(symbols) -> dict:
    """按需拉取指定币的费率（premiumIndex 全量后过滤，省调用）"""
    allf = fetch_funding_all()
    return {s: allf[s] for s in symbols if s in allf}


def persist_funding_subset(conn, funding, symbols, ts):
    """写 funding_history 并标记 last_funding_ts"""
    n = 0
    for sym in symbols:
        rate = funding.get(sym)
        if rate is None:
            continue
        conn.execute("INSERT OR IGNORE INTO funding_history (symbol, ts, funding_rate) VALUES (?,?,?)",
                     (sym, ts, rate))
        conn.execute("UPDATE contracts_track SET funding_rate=? WHERE symbol=?", (rate, sym))
        conn.execute("UPDATE contract_meta SET last_funding_ts=? WHERE symbol=?", (ts, sym))
        n += 1
    conn.commit()
    return n


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
    """热 7 天→1h 降采样；OI/LSR 先 1h 降采样再删 30 天；funding 保留 30 天；VACUUM 失败仅告警

    2026-10-12 批次D 修复：原先 open_interest / long_short_ratio 到 30 天直接 DELETE
    无降采样 → 每 30 天永久丢 227 万行（9/12 前的已丢）。现在先聚合到 *_hourly 再删。
    """
    hot = now_ms - 7 * 86400000
    cutoff = now_ms - 30 * 86400000
    conn.execute("""INSERT OR IGNORE INTO contracts_history_hourly
        SELECT symbol, (ts/3600000)*3600000, AVG(price), MAX(volume_24h), AVG(price_change_24h)
        FROM contracts_history WHERE ts < ? GROUP BY symbol, (ts/3600000)""", (hot,))
    conn.execute("DELETE FROM contracts_history WHERE ts < ?", (hot,))
    # ── OI / LSR：先降采样（1h avg/max/min）再删 ──
    conn.execute("""INSERT OR IGNORE INTO open_interest_hourly
        SELECT symbol, (ts/3600000)*3600000, AVG(oi), MAX(oi), MIN(oi)
        FROM open_interest WHERE ts < ? GROUP BY symbol, (ts/3600000)""", (cutoff,))
    conn.execute("DELETE FROM open_interest WHERE ts < ?", (cutoff,))
    conn.execute("""INSERT OR IGNORE INTO long_short_ratio_hourly
        SELECT symbol, (ts/3600000)*3600000, AVG(lsr), MAX(lsr), MIN(lsr)
        FROM long_short_ratio WHERE ts < ? GROUP BY symbol, (ts/3600000)""", (cutoff,))
    conn.execute("DELETE FROM long_short_ratio WHERE ts < ?", (cutoff,))
    conn.execute("DELETE FROM funding_history WHERE ts < ?", (cutoff,))
    # ── 三类新数据（2026-10-12 R3）：先 1h 降采样再删 30 天外，防无保留策略增长 ──
    #    实测 804 币 × 15min × 365 天 = 8,450 万行/年 ≈ 4.4GB/年，库已 849MB。
    #    保留期与 OI/LSR 口径一致（30 天热 + 2 年 1h 归档），便于后续因子回测。
    conn.execute("""INSERT OR IGNORE INTO top_position_ratio_hourly
        SELECT symbol, (ts/3600000)*3600000, AVG(ls_ratio), MAX(ls_ratio), MIN(ls_ratio)
        FROM top_position_ratio WHERE ts < ? GROUP BY symbol, (ts/3600000)""", (cutoff,))
    conn.execute("DELETE FROM top_position_ratio WHERE ts < ?", (cutoff,))
    conn.execute("""INSERT OR IGNORE INTO top_account_ratio_hourly
        SELECT symbol, (ts/3600000)*3600000, AVG(ls_ratio), MAX(ls_ratio), MIN(ls_ratio)
        FROM top_account_ratio WHERE ts < ? GROUP BY symbol, (ts/3600000)""", (cutoff,))
    conn.execute("DELETE FROM top_account_ratio WHERE ts < ?", (cutoff,))
    conn.execute("""INSERT OR IGNORE INTO taker_ratio_hourly
        SELECT symbol, (ts/3600000)*3600000, AVG(buy_sell_ratio), MAX(buy_sell_ratio), MIN(buy_sell_ratio)
        FROM taker_ratio WHERE ts < ? GROUP BY symbol, (ts/3600000)""", (cutoff,))
    conn.execute("DELETE FROM taker_ratio WHERE ts < ?", (cutoff,))
    # ── 降采样表保留上限（防无上限增长，实测 hourly 表 +472MB/年）──
    conn.execute("DELETE FROM contracts_history_hourly WHERE ts < ?", (now_ms - 730 * 86400000,))
    conn.execute("DELETE FROM open_interest_hourly WHERE ts < ?", (now_ms - 730 * 86400000,))
    conn.execute("DELETE FROM long_short_ratio_hourly WHERE ts < ?", (now_ms - 730 * 86400000,))
    conn.execute("DELETE FROM top_position_ratio_hourly WHERE ts < ?", (now_ms - 730 * 86400000,))
    conn.execute("DELETE FROM top_account_ratio_hourly WHERE ts < ?", (now_ms - 730 * 86400000,))
    conn.execute("DELETE FROM taker_ratio_hourly WHERE ts < ?", (now_ms - 730 * 86400000,))
    conn.commit()
    try:
        conn.execute("VACUUM")
    except Exception as e:
        logger.warning("VACUUM skip (WAL 下可稍后重试): %s", e)


# ── 主循环 ──────────────────────────────────

def main_loop():
    init_db()
    cycle = 0
    _funding_intervals = {}          # 2026-10-12 批次B: {symbol: interval_hours}
    _ratio_pool = []                 # 2026-10-12 批次B: Top N 采集池
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
                # 2026-10-12 批次B: 大户持仓比/大户账户比/taker 买卖量比
                #   只取成交额 Top300（3 端点 × 300 = 900 req/15min，落在限流内）
                _ratio_pool = [r[0] for r in conn.execute(
                    "SELECT symbol FROM contracts_track WHERE quote_volume > 0 "
                    "ORDER BY quote_volume DESC LIMIT ?", (RATIO_TOP,))]
                persist_top_position(conn, fetch_top_position_batch(_ratio_pool), ts)
                persist_top_account(conn, fetch_top_account_batch(_ratio_pool), ts)
                persist_taker(conn, fetch_taker_batch(_ratio_pool), ts)
                promote_observing(conn, ts)               # 15min 检查转正（含 R1 提前转正）
                conn.close()

            now = datetime.now(timezone.utc)
            # 2026-10-12 批次B: 费率改为按各币真实结算周期采集（原先固定 23/07/15 三点
            #   → 468 个 4h 结算币被隔期采样，funding_flip 语义失真）
            if cycle % OI_EVERY == 0:
                if not _funding_intervals or now.hour == 0:
                    _funding_intervals = load_funding_intervals()
                conn = connect()
                if _funding_intervals:
                    conn.executemany(
                        "UPDATE contract_meta SET funding_interval=? WHERE symbol=?",
                        [(v, k) for k, v in _funding_intervals.items()])
                    conn.commit()
                    _due = funding_due_symbols(conn, _funding_intervals, ts)
                    if _due:
                        persist_funding_subset(conn, fetch_funding_for(_due), _due, ts)
                        logger.info(json.dumps({"event": "funding_pull", "n": len(_due)}))
                else:
                    # 回退：fundingInfo 不可用时保持旧行为（结算前 5min 全量）
                    if now.hour in (23, 7, 15) and now.minute >= 55:
                        persist_funding(conn, fetch_funding_all(), ts)
                conn.close()

            if cycle % DEPTH_EVERY == 0:                  # 1h
                conn = connect()
                for row in fetch_depth():
                    conn.execute("INSERT OR IGNORE INTO contracts_depth VALUES (?,?,?,?,?)",
                                 (row[0], ts) + row[1:])
                conn.commit()
                conn.close()

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
