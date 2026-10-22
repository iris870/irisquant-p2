#!/usr/bin/env python3
"""P2 健康检查 — 输出单行 JSON，critical 时推 TG alert 通道（30min 冷却去重）"""
import json
import os
import sqlite3
import time
import urllib.request

DB_PATH = "/root/workspace/analysis_notes/market_radar.db"
ENV_PATH = "/root/irisquant/.env"
STATE_PATH = "/root/workspace/analysis_notes/.p2_healthcheck_state.json"
ALERT_COOLDOWN_MS = 30 * 60 * 1000  # 30 分钟去重，防每 10 分钟轰炸


def _read_alert_env():
    """读 irisquant .env 的 TG alert 通道（iris0memebot → 私聊 7177920417）"""
    token = chat_id = ""
    try:
        with open(ENV_PATH) as f:
            for line in f:
                line = line.strip()
                if line.startswith("TG_BOT_TOKEN_ALERT="):
                    token = line.split("=", 1)[1].strip()
                elif line.startswith("TG_CHAT_ID_ALERT="):
                    chat_id = line.split("=", 1)[1].strip()
    except Exception:
        pass
    return token, chat_id


def _send_tg(token, chat_id, text):
    """标准库 urllib 调 Telegram sendMessage，不引入 httpx（p2 独立脚本）"""
    try:
        url = f"https://api.telegram.org/bot{token}/sendMessage"
        data = json.dumps({"chat_id": chat_id, "text": text}).encode()
        req = urllib.request.Request(url, data=data,
                                     headers={"Content-Type": "application/json"})
        urllib.request.urlopen(req, timeout=10)
        return True
    except Exception:
        return False


def _load_state():
    try:
        with open(STATE_PATH) as f:
            return json.load(f)
    except Exception:
        return {}


def _save_state(state):
    try:
        with open(STATE_PATH, "w") as f:
            json.dump(state, f)
    except Exception:
        pass


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
    status = "ok" if not errors else "critical"
    out = {"event": "healthcheck", "ts": now_ms, "status": status, "errors": errors,
           "checks": {"last_history_ms": last_hist, "last_oi_ms": last_oi,
                      "last_signal_ms": last_sig, "signals_24h": sig_24h}}
    print(json.dumps(out))

    # ── TG 告警：critical + 冷却去重 ──
    if status == "critical":
        state = _load_state()
        last_alert = state.get("last_critical_alert_ts", 0)
        if now_ms - last_alert > ALERT_COOLDOWN_MS:
            token, chat_id = _read_alert_env()
            if token and chat_id:
                text = "⚠️ P2 健康检查异常\n" + "\n".join(
                    f"• {e}" for e in errors)
                if _send_tg(token, chat_id, text):
                    _save_state({"last_critical_alert_ts": now_ms})
    else:
        # 恢复 ok：清告警状态，下次再 critical 立即告警
        try:
            if os.path.exists(STATE_PATH):
                os.remove(STATE_PATH)
        except Exception:
            pass


if __name__ == "__main__":
    main()
