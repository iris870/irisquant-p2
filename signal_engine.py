#!/usr/bin/env python3
"""P2 信号引擎 v2.2 — 每小时整点跑一次，跑完 sys.exit(0)（配合 PM2 cron_restart）"""
import json
import logging
import sqlite3
import sys
import time

DB_PATH = "/root/workspace/analysis_notes/market_radar.db"

# R2: 命中阈值按信号类型差异化（确认项 3）
HIT_THRESHOLD = {
    "price_surge": 0.02,
    "oi_spike": 0.015,
    "funding_flip": 0.01,
    "strong_momentum": 0.03,
    "new_listing": 0.05,
    "volume_price_divergence": 0.015,
    "sustained_momentum": 0.02,
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
    conn.commit()
    conn.close()
    sys.exit(0)     # R3: 跑完即退，PM2 到整点再拉起


def interval_volume(arr) -> float:
    """F4: 窗口内滚动成交额 max-min ≈ 区间增量"""
    vols = [r["vol"] for r in arr if r["vol"]]
    return (max(vols) - min(vols)) if len(vols) >= 2 else 0.0


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    run()
