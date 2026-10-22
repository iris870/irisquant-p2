#!/usr/bin/env python3
"""P2 健康检查 — 输出单行 JSON，便于 jq 与后续接通道"""
import json
import sqlite3
import time

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
        "SELECT COUNT(*) FROM signals WHERE ts > ?", (now_ms - 24 * 3600 * 1000,)).fetchone()[0]
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
