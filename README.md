# P2 全合约跟踪层（独立于 IrisQuant 主仓库）

P2 = 币安全市场合约持续跟踪子系统。发现/采集/信号/消费四层，数据存 market_radar.db。

## 生产脚本（被 PM2 进程引用，本仓库只跟踪这些）

| 文件 | 进程 | 模式 | 频率 |
|---|---|---|---|
| contracts_tracker.py | p2-tracker | 常驻 daemon | Ticker 5min / OI+LSR 15min / Funding 8h / Depth 1h |
| signal_engine.py | p2-signal | 跑完即退 | 每小时整点 cron_restart |
| resonance_engine.py | p2-resonance | 跑完即退 | 每小时第 5 分钟 |
| alpha_bsc_tag.py | p2-tag | 跑完即退 | 每小时整点 |
| p2_healthcheck.py | p2-healthcheck | 跑完即退 | 每 10 分钟 |

## 生产数据（不版本管理，留原位）

- market_radar.db（1.4G，WAL 模式）—— 11+ 表，ts 毫秒 epoch
- factor_panel.db
- market_radar.backup_*.db

## 运维

```bash
pm2 restart p2-tracker          # 唯一常驻进程
tail /root/.pm2/logs/p2-signal-out-147.log   # 信号摘要
tail /root/.pm2/logs/p2-healthcheck-out-148.log  # 健康检查 JSON
```

完整运维手册见 skill：p2-contract-tracking。
方案契约源：P2_full_contract_tracking_plan_v2.2.md（本仓库跟踪）。
