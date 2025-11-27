# Scripts Directory

This directory contains setup, maintenance, and monitoring scripts for the trading bot.

## 📁 Directory Structure

```
scripts/
├── deploy.sh              # Initial deployment script for Ubuntu
├── setup_schedule.sh      # Configure automatic scheduling (cron)
├── pre_download_csv.sh    # Pre-downloads Dhan CSV before market
├── monitor.py             # Health monitoring and diagnostics
├── verify_deployment.py   # Tests the bot configuration
├── sandbox_test.py        # Sandbox testing script
└── README.md             # This file
```

## 🚀 Main Scripts

### Deployment

**`deploy.sh`** - One-time deployment setup
```bash
cd /opt/trading_bot
./scripts/deploy.sh
```
- Installs system dependencies
- Creates virtual environment
- Installs Python packages
- Sets up directories
- Validates configuration

### Scheduling

**`setup_schedule.sh`** - Configure automatic start/stop
```bash
cd /opt/trading_bot
./scripts/setup_schedule.sh
```
- Sets up cron jobs for:
  - 8:50 AM: CSV pre-download
  - 9:00 AM: Bot start
  - 3:35 PM: Bot stop
  - Every hour: Health checks
  - Weekly: Log cleanup

### Monitoring

**`monitor.py`** - Health check and diagnostics
```bash
cd /opt/trading_bot
python scripts/monitor.py
```
- Checks if bot is running
- Validates log files
- Monitors error frequency
- Checks disk space
- Verifies CSV freshness

## 🔧 Maintenance Scripts

### CSV Pre-download

**`pre_download_csv.sh`** - Downloads CSV before market
```bash
cd /opt/trading_bot
./scripts/pre_download_csv.sh
```
- Called automatically by cron at 8:50 AM
- Downloads ~500MB Dhan master CSV
- Logs progress to `logs/cron.log`

## 🧪 Testing Scripts

### Deployment Verification

**`verify_deployment.py`** - Test bot configuration
```bash
cd /opt/trading_bot
python scripts/verify_deployment.py
```
- Tests signal parsing
- Validates bridge logic
- Checks risk calculations
- Mocks API calls

### Sandbox Test

**`sandbox_test.py`** - Full simulation test
```bash
cd /opt/trading_bot
python scripts/sandbox_test.py
```
- Runs complete signal flow
- Tests with mock data
- Validates order payload

## 📝 Usage Examples

### First Time Setup
```bash
# 1. Deploy the bot
./scripts/deploy.sh

# 2. Configure .env file
nano .env

# 3. Set up automatic scheduling
./scripts/setup_schedule.sh

# 4. Verify everything works
python scripts/monitor.py
```

### Daily Monitoring
```bash
# Check bot health
python scripts/monitor.py

# View logs
tail -f logs/trade_logs.log
```

### Testing Changes
```bash
# Test deployment
python scripts/verify_deployment.py

# Full simulation
python scripts/sandbox_test.py
```

## 🔄 Updating Cron Jobs

If you move the bot or change paths, re-run:
```bash
./scripts/setup_schedule.sh
```

This will update all cron job paths automatically.

## 📖 Documentation

For detailed information, see:
- [DEPLOYMENT.md](../DEPLOYMENT.md) - Deployment guide
- [SCHEDULING.md](../SCHEDULING.md) - Scheduling details
- [CODE_REVIEW.md](../CODE_REVIEW.md) - Code analysis

## ⚠️ Important Notes

- All scripts use absolute paths (safe to run from anywhere)
- Scripts automatically detect project directory
- Logs are written to `logs/` in project root
- Scripts are executable (chmod +x already set)

## 🆘 Troubleshooting

**Script not found?**
```bash
# Make sure you're in project directory
cd /opt/trading_bot

# Or use absolute path
/opt/trading_bot/scripts/script_name.sh
```

**Permission denied?**
```bash
chmod +x scripts/*.sh
chmod +x scripts/*.py
```

**Cron jobs not working?**
```bash
# Check crontab
crontab -l

# View cron logs
tail -f logs/cron.log

# Re-run setup
./scripts/setup_schedule.sh
```

---

**All scripts are production-ready and tested!** ✅
