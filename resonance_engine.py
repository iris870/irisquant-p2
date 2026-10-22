#!/usr/bin/env python3
"""P2 共振信号 + 风控过滤引擎 v1.0 — 每小时整点跑一次，跑完 sys.exit(0)

数据流（纯展示层，不碰真实资金，不改变现有交易链路）：
  signals + signal_snapshots + contracts_track + contracts_depth + signal_performance (只读)
    ↓ 共振聚合：同 symbol 4h 窗口内信号 ≥2 聚合成一条，复用 signal_engine 的 weight 打分
  signal_resonance (写)
    ↓ R1~R6 风控过滤
  signal_risk_filter (写)

打分 weight 单一权威源 = signal_engine.SIGNAL_DEFS（不另造数值，避免两套分叉）。

PM2: cron_restart='0 * * * *', autorestart=false（跟 p2-signal 同步，在其后跑）。
"""

import json
import logging
import sqlite3
import sys
import time

DB_PATH = "/root/workspace/analysis_notes/market_radar.db"

# 打分 weight 单一权威源（signal_engine.SIGNAL_DEFS）。只取 weight，direction 已由 signal_engine
# 算好存进 signals.direction，这里不重算。
from signal_engine import SIGNAL_DEFS  # noqa: E402

WINDOW = "4h"
WINDOW_MS = 4 * 3600000
MIN_RESONANCE = 2            # 至少几个信号才算共振
R3_MIN_VOLUME = 10_000_000   # 24h 成交额 < 1000万 USDT → 剔除
R4_SPREAD_WARN = 0.005       # spread_pct > 0.5% → 预警（不硬卡，标 warn）
R5_STOP_LOSS = -0.03         # 现价跌破 entry_price -3% → 剔除
R6_FUNDING_EXTREME = 0.003   # |funding_rate| > 0.3% → 降权 50%

logger = logging.getLogger("p2-resonance")


def connect():
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.execute("PRAGMA busy_timeout=30000")
    conn.row_factory = sqlite3.Row
    return conn


def sample_factor(cnt):
    """样本置信度：按该信号类型在 signal_performance 的历史触发次数映射。"""
    if cnt >= 10000:
        return 1.0
    if cnt >= 1000:
        return 0.8
    return 0.5


def classify_combo(signal_list):
    """按组合指纹分类 → (direction, combo_type)
    vpd 是方向判别器：vpd(long)=放量下跌吸筹做多 / vpd(short)=缩量上涨衰竭做空 /
    无 vpd 纯动量=顶部弱做空。回测依据见 plans/irislens_共振组合重构_20260927.md
    """
    types = {}
    for s in signal_list:
        types.setdefault(s.get("signal_type", ""), set()).add(s.get("direction", ""))
    vpd = types.get("volume_price_divergence", set())
    if "long" in vpd:
        # 高位 vpd：与追涨信号同现 = 高位放量出货，非低位吸筹。
        # 回测（2026-10-16 组合回测）：做多 4h 46.9%/24h 41.0% 负 edge；
        # 转做空 1h/4h 也仅 48.6%/52.8% 无 edge（24h 58.9% 是均值回归假象）。
        # → 判 neutral/vpd_high 丢弃，不进任何消费；纯吸筹(无追涨)保留做多 edge。
        _chase = {"price_surge", "sustained_momentum", "oi_spike", "strong_momentum"}
        if any(t in types for t in _chase):
            return "neutral", "vpd_high"
        return "long", "vpd_accumulation"      # 纯放量下跌吸筹 → 做多
    if "short" in vpd:
        return "short", "vpd_exhaustion"       # 缩量上涨衰竭 → 做空
    if "oi_spike" in types and "strong_momentum" in types:
        return "short", "momentum_extreme"     # 全动量堆叠 → 做空
    if "price_surge" in types or "sustained_momentum" in types:
        return "short", "momentum_top"         # 纯动量顶部 → 做空
    return "neutral", "other"


def run():
    conn = connect()
    now = int(time.time() * 1000)
    window_start = now // 3600000 * 3600000  # 对齐整点（每整点一个窗口）

    # 样本量：signal_performance 按类型历史触发次数（经验充分度）
    sample_cnt = {r["signal_type"]: r["cnt"] for r in conn.execute(
        "SELECT signal_type, COUNT(*) AS cnt FROM signal_performance GROUP BY signal_type")}

    # 最近 4h 触发的信号（ts 在窗口内），JOIN snapshots 拿触发价
    rows = conn.execute("""
        SELECT s.id, s.symbol, s.signal_type, s.direction, s.strength, s.ts
        FROM signals s
        WHERE s.ts > ?
        ORDER BY s.symbol, s.ts
    """, (now - WINDOW_MS,)).fetchall()

    # 按 symbol 分组
    groups = {}
    for r in rows:
        groups.setdefault(r["symbol"], []).append(r)

    # 当前价/成交量/费率（R3/R5/R6 用）
    track = {r["symbol"]: r for r in conn.execute(
        "SELECT symbol, price, quote_volume, funding_rate FROM contracts_track")}

    # 最新 spread（R4 用；contracts_depth 只采 Top50，很多 symbol 没有 → unknown）
    spread = {}
    for r in conn.execute("""
        SELECT symbol, spread_pct FROM contracts_depth
        WHERE (symbol, ts) IN (SELECT symbol, MAX(ts) FROM contracts_depth GROUP BY symbol)
    """):
        spread[r["symbol"]] = r["spread_pct"]

    n_resonance = 0
    n_pass = n_reverse = n_reject = 0

    for sym, sigs in groups.items():
        if len(sigs) < MIN_RESONANCE:
            continue

        # 信号列表 + 基础分（复用 SIGNAL_DEFS weight）
        signal_list = []
        raw_score = 0.0
        for s in sigs:
            st = s["signal_type"]
            w = SIGNAL_DEFS.get(st, {}).get("weight", 0)
            raw_score += w
            signal_list.append({
                "signal_type": st,
                "direction": s["direction"],
                "strength": s["strength"],
                "ts": s["ts"],
                "weight": w,
            })

        # 样本系数：取参与共振信号里 min cnt（避免被大样本信号拉高）
        cnts = [sample_cnt.get(s["signal_type"], 0) for s in sigs]
        min_cnt = min(cnts) if cnts else 0
        sf = sample_factor(min_cnt)

        final_score = round(raw_score * sf, 2)

        # 方向 + 组合分类：vpd 是方向判别器（不再按 weight 最大信号判方向）
        direction, combo_type = classify_combo(signal_list)

        # entry_price：symbol 当前价（共振首次创建基准，R5 用）
        entry_price = track[sym]["price"] if sym in track else None

        # 写 signal_resonance（upsert：同 symbol 同整点幂等，entry_price 首次固定不覆盖）
        cur = conn.execute("""
            INSERT INTO signal_resonance
              (symbol, window, window_start, signal_list, resonance_count,
               raw_score, sample_factor, final_score, direction, combo_type, entry_price, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(symbol, window_start) DO UPDATE SET
              signal_list=excluded.signal_list, resonance_count=excluded.resonance_count,
              raw_score=excluded.raw_score, sample_factor=excluded.sample_factor,
              final_score=excluded.final_score, direction=excluded.direction,
              combo_type=excluded.combo_type,
              created_at=excluded.created_at
        """, (sym, WINDOW, window_start, json.dumps(signal_list), len(sigs),
              raw_score, sf, final_score, direction, combo_type, entry_price, now))
        rid = conn.execute(
            "SELECT id FROM signal_resonance WHERE symbol=? AND window_start=?",
            (sym, window_start)).fetchone()[0]
        n_resonance += 1

        # ── R1~R6 风控过滤 ──
        tr = track.get(sym)
        # R1: 方向冲突（long 与 short 并存）
        dirs = {s["direction"] for s in sigs if s["direction"] in ("long", "short")}
        r1 = "reverse" if dirs == {"long", "short"} else "pass"
        # R2: min cnt < 1000 降权 50%
        r2 = "downweight" if min_cnt < 1000 else "pass"
        # R3: 24h 成交额不足
        vol = tr["quote_volume"] if tr else None
        r3 = "reject" if (vol is not None and vol < R3_MIN_VOLUME) else "pass"
        # R4: 滑点粗筛（spread 预警，不硬卡）
        sp = spread.get(sym)
        if sp is None:
            r4 = "unknown"
        elif sp > R4_SPREAD_WARN:
            r4 = "warn"
        else:
            r4 = "pass"
        # R5: 现价跌破 entry_price -3%
        if entry_price and tr and tr["price"] and tr["price"] < entry_price * (1 + R5_STOP_LOSS):
            r5 = "reject"
        else:
            r5 = "pass"
        # R6: 资金费率极端
        fr = tr["funding_rate"] if tr else None
        r6 = "downweight" if (fr is not None and abs(fr) > R6_FUNDING_EXTREME) else "pass"

        # 汇总
        reasons = []
        if r3 == "reject":
            reasons.append("R3成交量不足")
        if r5 == "reject":
            reasons.append("R5跌破止损-3%")
        if r1 == "reverse":
            reasons.append("R1方向冲突")
        if r4 == "warn":
            reasons.append("R4滑点预警")

        down_ratio = (0.5 if r2 == "downweight" else 1.0) * (0.5 if r6 == "downweight" else 1.0)
        if r3 == "reject" or r5 == "reject":
            final_result = "reject"
            adjusted = None
        elif r1 == "reverse":
            final_result = "reverse"
            adjusted = round(final_score * down_ratio, 2)
        else:
            final_result = "pass"
            adjusted = round(final_score * down_ratio, 2)

        if final_result == "pass":
            n_pass += 1
        elif final_result == "reverse":
            n_reverse += 1
        else:
            n_reject += 1

        conn.execute("""
            INSERT INTO signal_risk_filter
              (symbol, resonance_id, r1_result, r2_result, r3_result, r4_result,
               r5_result, r6_result, final_result, reject_reason, adjusted_score, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(resonance_id) DO UPDATE SET
              r1_result=excluded.r1_result, r2_result=excluded.r2_result,
              r3_result=excluded.r3_result, r4_result=excluded.r4_result,
              r5_result=excluded.r5_result, r6_result=excluded.r6_result,
              final_result=excluded.final_result, reject_reason=excluded.reject_reason,
              adjusted_score=excluded.adjusted_score, created_at=excluded.created_at
        """, (sym, rid, r1, r2, r3, r4, r5, r6, final_result,
              ",".join(reasons) if reasons else None, adjusted, now))

    # 清理 24h 前旧记录
    cutoff = now - 24 * 3600000
    conn.execute("DELETE FROM signal_resonance WHERE created_at < ?", (cutoff,))
    conn.execute("DELETE FROM signal_risk_filter WHERE created_at < ?", (cutoff,))

    conn.commit()
    conn.close()

    print(json.dumps({
        "event": "resonance_summary", "ts": now,
        "groups": len(groups), "resonance": n_resonance,
        "pass": n_pass, "reverse": n_reverse, "reject": n_reject,
    }))
    sys.exit(0)  # 跑完即退，PM2 到下个整点再拉起


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    run()
