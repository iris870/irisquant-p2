#!/usr/bin/env python3
"""Alpha/BSC 双组标记引擎 v2.0 — 每小时整点跑一次，跑完 sys.exit(0)

两组标记（tag_type 区分，表存在即命中）：

  组A fut_only（BSC · 有合约 · 无现货）：
      合约-only 差集 ∩ Alpha(chainName=='BSC') ∩ 合约价 vs Alpha价 偏差<3%

  组B fut_spot（BSC · 有合约 · 有现货）：
      合约∩现货 交集 ∩ Alpha(chainName=='BSC')
      ∩ ( Alpha.cexCoinName==base 且 listingCex            ← 币安 CEX 命名，权威
          或 合约价 vs Alpha价 偏差<3% )                    ← 补 Alpha 端 stale 的币

权威数据源（全部币安官方，不碰 CoinGecko/GMGN）：
  - 链判定 + 市值/流通量 = 币安 Alpha API
  - 有合约/有现货 = fapi/spot exchangeInfo（PERPETUAL 排除 TRADIFI）
  - 撞车消歧 = 价格交叉验证

实测规模（2026-10-12）：合约-only 168 / 合约+现货 357 / Alpha 682
  → 组A 95 / 组B 63（合计 158 会员）

异常保护（API 抖动不清空旧 badge）：
  - 任一数据源请求失败 → 跳过本轮，保留旧数据
  - Alpha 代币数 < MIN_ALPHA（正常 682）→ 判定抖动，跳过
  - 合约-only 差集 < MIN_FUT_ONLY（正常 168）→ 判定抖动，跳过
  - 全部成功才在单事务内全量重算（DELETE + INSERT）

PM2: p2-tag，cron_restart='0 * * * *'，autorestart=false（跑完即退）。
"""

import json
import logging
import sqlite3
import sys
import time
import urllib.request
from collections import defaultdict

DB_PATH = "/root/workspace/analysis_notes/market_radar.db"

ALPHA_LIST_URL = ("https://www.binance.com/bapi/defi/v1/public/"
                  "wallet-direct/buw/wallet/cex/alpha/all/token/list")
FUTURES_EXCHANGE_URL = "https://fapi.binance.com/fapi/v1/exchangeInfo"
SPOT_EXCHANGE_URL = "https://api.binance.com/api/v3/exchangeInfo"
TICKER_URL = "https://fapi.binance.com/fapi/v1/ticker/24hr"

PRICE_THRESH = 0.03     # 撞车消歧：价格偏差 <3% 才认
MIN_ALPHA = 500         # 正常 682；低于此判定 API 抖动
MIN_FUT_ONLY = 100      # 正常 168；低于此判定 API 抖动

HEADERS = {"User-Agent": "Mozilla/5.0", "Accept": "application/json"}

logger = logging.getLogger("p2-tag")


def _get(url, timeout=25):
    req = urllib.request.Request(url, headers=HEADERS)
    return json.loads(urllib.request.urlopen(req, timeout=timeout).read())


def _f(v):
    try:
        return float(v)
    except (TypeError, ValueError):
        return 0.0


def connect():
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.execute("PRAGMA busy_timeout=30000")
    return conn


def ensure_schema(conn):
    """表结构升级（幂等）：新增 tag_type / contract_address / circ_supply / total_supply 四列，
    以及 online_tge / online_airdrop / listing_cex / holders 四列（2026-10-15 妖币基因）。

    旧表（7 列）缺这些列 → 逐个 ADD COLUMN；已存在则跳过。
    不新建表、不重建表（现有消费方 token_radar/dashboard.py 用显式列名 SELECT，加列零破坏）。
    """
    have = {r[1] for r in conn.execute("PRAGMA table_info(contract_tag)")}
    for col, ddl in (
        ("tag_type", "ALTER TABLE contract_tag ADD COLUMN tag_type TEXT DEFAULT 'fut_only'"),
        ("contract_address", "ALTER TABLE contract_tag ADD COLUMN contract_address TEXT DEFAULT ''"),
        ("circ_supply", "ALTER TABLE contract_tag ADD COLUMN circ_supply REAL DEFAULT 0"),
        ("total_supply", "ALTER TABLE contract_tag ADD COLUMN total_supply REAL DEFAULT 0"),
        # 2026-10-15 妖币基因四字段（Alpha API 已有，之前丢弃）
        ("online_tge", "ALTER TABLE contract_tag ADD COLUMN online_tge INTEGER DEFAULT 0"),
        ("online_airdrop", "ALTER TABLE contract_tag ADD COLUMN online_airdrop INTEGER DEFAULT 0"),
        ("listing_cex", "ALTER TABLE contract_tag ADD COLUMN listing_cex INTEGER DEFAULT 0"),
        ("holders", "ALTER TABLE contract_tag ADD COLUMN holders REAL DEFAULT 0"),
    ):
        if col not in have:
            conn.execute(ddl)
    conn.commit()


def run():
    now = int(time.time() * 1000)

    # ── 1. 拉数据源（任一失败即跳过本轮，保留旧 badge）──
    try:
        alphas = _get(ALPHA_LIST_URL).get("data", [])
    except Exception as e:
        print(json.dumps({"event": "tag_summary", "ts": now, "status": "skip",
                          "reason": f"alpha_api_fail: {e}"}))
        sys.exit(0)

    if not isinstance(alphas, list) or len(alphas) < MIN_ALPHA:
        print(json.dumps({"event": "tag_summary", "ts": now, "status": "skip",
                          "reason": f"alpha_too_few: {len(alphas)}"}))
        sys.exit(0)

    try:
        fut = {s["baseAsset"]: s.get("onboardDate", 0)
               for s in _get(FUTURES_EXCHANGE_URL)["symbols"]
               if s.get("status") == "TRADING" and s.get("contractType") == "PERPETUAL"}
        spot = {s["baseAsset"] for s in _get(SPOT_EXCHANGE_URL)["symbols"]
                if s.get("status") == "TRADING"}
        tickers = {t["symbol"][:-4]: _f(t["lastPrice"])
                   for t in _get(TICKER_URL) if t["symbol"].endswith("USDT")}
    except Exception as e:
        print(json.dumps({"event": "tag_summary", "ts": now, "status": "skip",
                          "reason": f"exchange_api_fail: {e}"}))
        sys.exit(0)

    fut_only = sorted(set(fut) - spot)          # 组A 基础池
    fut_and_spot = sorted(set(fut) & spot)      # 组B 基础池
    if len(fut_only) < MIN_FUT_ONLY:
        print(json.dumps({"event": "tag_summary", "ts": now, "status": "skip",
                          "reason": f"fut_only_too_few: {len(fut_only)}"}))
        sys.exit(0)

    # ── 2. Alpha 按 symbol 分组（symbol 不唯一，撞车消歧用）──
    by_sym = defaultdict(list)
    for t in alphas:
        by_sym[(t.get("symbol") or "").upper()].append(t)

    def _dev(alpha_price, fut_price):
        """价格偏差（小数）；无法比较返回 None"""
        if fut_price > 0 and alpha_price > 0:
            return abs(alpha_price - fut_price) / fut_price
        return None

    def _row(base, t):
        return {
            "symbol": base + "USDT",
            "base_asset": base,
            "name": t.get("name", ""),
            "chain": t.get("chainName", ""),
            "market_cap": _f(t.get("marketCap")),
            "fdv": _f(t.get("fdv")),
            "contract_address": t.get("contractAddress") or "",
            "circ_supply": _f(t.get("circulatingSupply")),
            "total_supply": _f(t.get("totalSupply")),
            # 2026-10-15 妖币基因四字段
            "online_tge": 1 if t.get("onlineTge") else 0,
            "online_airdrop": 1 if t.get("onlineAirdrop") else 0,
            "listing_cex": 1 if t.get("listingCex") else 0,
            "holders": _f(t.get("holders")),
        }

    tags = []

    # ── 3a. 组A：BSC + 合约-only + 价格交叉验证 <3% ──
    for base in fut_only:
        pf = tickers.get(base, 0.0)
        for t in by_sym.get(base, []):
            if t.get("chainName") != "BSC":
                continue
            d = _dev(_f(t.get("price")), pf)
            if d is not None and d < PRICE_THRESH:
                tags.append(dict(_row(base, t), tag_type="fut_only"))
                break

    # ── 3b. 组B：BSC + 合约∩现货 + (cexCoinName 认币 或 价格<3%) ──
    for base in fut_and_spot:
        pf = tickers.get(base, 0.0)
        picked = None
        # 规则1：币安 CEX 命名（对现货币权威）
        for t in by_sym.get(base, []):
            if (t.get("chainName") == "BSC"
                    and (t.get("cexCoinName") or "").upper() == base
                    and t.get("listingCex")):
                picked = t
                break
        # 规则2：价格交叉验证（补 Alpha 端 stale、cexCoinName 为空的币，如 RESOLV/SOPH/TOWNS）
        if picked is None:
            for t in by_sym.get(base, []):
                if t.get("chainName") != "BSC":
                    continue
                d = _dev(_f(t.get("price")), pf)
                if d is not None and d < PRICE_THRESH:
                    picked = t
                    break
        if picked is not None:
            tags.append(dict(_row(base, picked), tag_type="fut_spot"))

    # ── 4. 全量重算（单事务：先清空再写，下架币自动消失）──
    conn = connect()
    try:
        ensure_schema(conn)
        conn.execute("DELETE FROM contract_tag")
        conn.executemany(
            "INSERT INTO contract_tag "
            "(symbol, base_asset, name, chain, market_cap, fdv, updated_at, "
            " tag_type, contract_address, circ_supply, total_supply, "
            " online_tge, online_airdrop, listing_cex, holders) "
            "VALUES (:symbol, :base_asset, :name, :chain, :market_cap, :fdv, :updated_at, "
            " :tag_type, :contract_address, :circ_supply, :total_supply, "
            " :online_tge, :online_airdrop, :listing_cex, :holders)",
            [dict(t, updated_at=now) for t in tags]
        )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    n_a = sum(1 for t in tags if t["tag_type"] == "fut_only")
    n_b = sum(1 for t in tags if t["tag_type"] == "fut_spot")
    print(json.dumps({"event": "tag_summary", "ts": now, "status": "ok",
                      "alpha": len(alphas), "fut_only": len(fut_only),
                      "fut_and_spot": len(fut_and_spot),
                      "tagged": len(tags), "tag_a": n_a, "tag_b": n_b}))
    sys.exit(0)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    run()
