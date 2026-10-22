// P2 全合约持续跟踪层 — PM2 配置（独立于 IrisQuant ecosystem）
// 2026-08-01 v2.2 定稿版
module.exports = {
  apps: [
    {
      name: 'p2-tracker',
      script: '/root/workspace/analysis_notes/contracts_tracker.py',
      interpreter: '/root/irisquant/venv/bin/python3',
      cwd: '/root/workspace/analysis_notes',
      instances: 1,
      exec_mode: 'fork',
      autorestart: true,
      max_restarts: 10,
      min_uptime: '10s',
      log_date_format: 'YYYY-MM-DD HH:mm:ss Z',
      cron_restart: '0 2 * * *',        // 每日 2 点重启兜底（配合每日归档）
      env: { PYTHONPATH: '/root/workspace/analysis_notes', LOG_LEVEL: 'info' }
    },
    {
      name: 'p2-signal',
      script: '/root/workspace/analysis_notes/signal_engine.py',
      interpreter: '/root/irisquant/venv/bin/python3',
      cwd: '/root/workspace/analysis_notes',
      instances: 1,
      exec_mode: 'fork',
      autorestart: false,                // R3: 跑完 sys.exit(0)，不自动重启
      cron_restart: '0 * * * *',         // 每小时整点拉起
      log_date_format: 'YYYY-MM-DD HH:mm:ss Z',
      env: { PYTHONPATH: '/root/workspace/analysis_notes', LOG_LEVEL: 'info' }
    },
    {
      name: 'p2-healthcheck',
      script: '/root/workspace/analysis_notes/p2_healthcheck.py',
      interpreter: '/root/irisquant/venv/bin/python3',
      cwd: '/root/workspace/analysis_notes',
      instances: 1,
      exec_mode: 'fork',
      autorestart: false,
      cron_restart: '*/10 * * * *',      // 每 10 分钟
      log_date_format: 'YYYY-MM-DD HH:mm:ss Z',
      env: { PYTHONPATH: '/root/workspace/analysis_notes', LOG_LEVEL: 'info' }
    },
    {
      name: 'p2-resonance',
      script: '/root/workspace/analysis_notes/resonance_engine.py',
      interpreter: '/root/irisquant/venv/bin/python3',
      cwd: '/root/workspace/analysis_notes',
      instances: 1,
      exec_mode: 'fork',
      autorestart: false,                // 跑完 sys.exit(0)，不自动重启
      cron_restart: '5 * * * *',         // 每小时整点后 5 分钟（signal_engine 之后）
      log_date_format: 'YYYY-MM-DD HH:mm:ss Z',
      env: { PYTHONPATH: '/root/workspace/analysis_notes', LOG_LEVEL: 'info' }
    },
    {
      name: 'p2-tag',
      script: '/root/workspace/analysis_notes/alpha_bsc_tag.py',
      interpreter: '/root/irisquant/venv/bin/python3',
      cwd: '/root/workspace/analysis_notes',
      instances: 1,
      exec_mode: 'fork',
      autorestart: false,                // 跑完 sys.exit(0)，不自动重启
      cron_restart: '0 * * * *',         // 每小时整点（跟 signal_engine 同步）
      log_date_format: 'YYYY-MM-DD HH:mm:ss Z',
      env: { PYTHONPATH: '/root/workspace/analysis_notes', LOG_LEVEL: 'info' }
    }
  ]
};
