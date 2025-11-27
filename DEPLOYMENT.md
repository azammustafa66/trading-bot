# Trading Bot - Deployment Guide

Complete guide to deploy the Trading Bot on Ubuntu droplet.

## 📋 Table of Contents

- [Prerequisites](#prerequisites)
- [Quick Start](#quick-start)
- [Detailed Setup](#detailed-setup)
- [Configuration](#configuration)
- [Running the Bot](#running-the-bot)
- [Monitoring](#monitoring)
- [Troubleshooting](#troubleshooting)
- [Maintenance](#maintenance)

---

## Prerequisites

### System Requirements

- **OS**: Ubuntu 20.04+ or Debian 11+
- **RAM**: Minimum 1GB (2GB recommended)
- **Disk**: At least 2GB free space (for CSV cache)
- **Python**: 3.9 or higher
- **Network**: Stable internet connection

### Required Accounts

1. **Telegram API Credentials**
   - Get from: https://my.telegram.org/apps
   - You'll need: API ID and API Hash

2. **Dhan Trading Account**
   - Get from: https://dhan.co
   - You'll need: Client ID and Access Token

3. **Telegram Channel**
   - The channel from which to receive trading signals
   - Must have access to this channel

---

## Quick Start

For experienced users, use the automated deployment script:

```bash
# Clone the repository
git clone <your-repo-url>
cd trading-bot

# Run deployment script
chmod +x deploy.sh
./deploy.sh
```

The script will guide you through the entire setup process.

---

## Detailed Setup

### Step 1: Update System

```bash
sudo apt update && sudo apt upgrade -y
```

### Step 2: Install Dependencies

```bash
sudo apt install -y python3 python3-pip python3-venv screen git curl
```

### Step 3: Clone Repository

```bash
cd ~
git clone <your-repo-url>
cd trading-bot
```

### Step 4: Create Virtual Environment

```bash
python3 -m venv .venv
source .venv/bin/activate
```

### Step 5: Install Python Packages

```bash
pip install --upgrade pip
pip install -r requirements.txt
```

### Step 6: Configure Environment

```bash
# Copy example environment file
cp .env.example .env

# Edit with your credentials
nano .env
```

Fill in your credentials:

```env
# Telegram Configuration
TELEGRAM_API_ID=your_api_id
TELEGRAM_API_HASH=your_api_hash
TARGET_CHANNEL=@your_channel

# Dhan Configuration
DHAN_CLIENT_ID=your_client_id
DHAN_ACCESS_TOKEN=your_access_token
```

Save with `Ctrl+X`, then `Y`, then `Enter`.

---

## Configuration

### Environment Variables

| Variable | Required | Description | Example |
|----------|----------|-------------|---------|
| `TELEGRAM_API_ID` | Yes | Telegram API ID | `12345678` |
| `TELEGRAM_API_HASH` | Yes | Telegram API Hash | `abcdef123456...` |
| `SESSION_NAME` | No | Telegram session name | `telegram_session` |
| `TARGET_CHANNEL` | Yes | Channel to monitor | `@tradingchannel` |
| `DHAN_CLIENT_ID` | Yes | Dhan client ID | `1234567890` |
| `DHAN_ACCESS_TOKEN` | Yes | Dhan access token | `eyJhbG...` |
| `DEDUPE_WINDOW_MINUTES` | No | Signal deduplication window | `60` |
| `LOG_LEVEL` | No | Logging level | `INFO` |
| `MAX_LOG_SIZE_MB` | No | Max log file size | `50` |
| `LOG_BACKUP_COUNT` | No | Number of log backups | `5` |

### Log Levels

- `DEBUG`: Detailed diagnostic information
- `INFO`: General information (default, recommended)
- `WARNING`: Warning messages only
- `ERROR`: Error messages only
- `CRITICAL`: Critical errors only

---

## Running the Bot

### Method 1: Using start_bot.sh (Recommended)

```bash
# Make script executable
chmod +x start_bot.sh

# Start the bot
./start_bot.sh start

# Check status
./start_bot.sh status

# View logs
./start_bot.sh logs

# Stop the bot
./start_bot.sh stop

# Restart the bot
./start_bot.sh restart
```

### Method 2: Using systemd (Auto-start on boot)

```bash
# Install as service
sudo cp trading-bot.service /etc/systemd/system/

# Edit the service file to replace YOUR_USERNAME
sudo nano /etc/systemd/system/trading-bot.service

# Reload systemd
sudo systemctl daemon-reload

# Enable auto-start on boot
sudo systemctl enable trading-bot

# Start the service
sudo systemctl start trading-bot

# Check status
sudo systemctl status trading-bot

# View logs
sudo journalctl -u trading-bot -f
```

### Method 3: Manual Run (Development)

```bash
# Activate virtual environment
source .venv/bin/activate

# Run directly
python main.py
```

---

## Monitoring

### Health Checks

Run the health monitor:

```bash
# Make executable
chmod +x monitor.py

# Run health check
python monitor.py
```

The health check will verify:
- Process is running
- Log files are being written
- No excessive errors
- Disk space is available
- Cache is up to date

### View Logs

```bash
# View main logs
tail -f logs/trade_logs.log

# View error logs only
tail -f logs/errors.log

# View systemd logs
sudo journalctl -u trading-bot -f

# Search logs for specific text
grep "ERROR" logs/trade_logs.log
```

### Log Rotation

Logs automatically rotate when they reach 50MB (configurable). Old logs are kept as:
- `trade_logs.log.1`
- `trade_logs.log.2`
- etc.

---

## Troubleshooting

### Bot Not Starting

**Check logs:**
```bash
tail -50 logs/errors.log
```

**Common issues:**

1. **Missing credentials**
   - Check `.env` file has all required variables
   - Verify credentials are correct

2. **Virtual environment not activated**
   ```bash
   source .venv/bin/activate
   ```

3. **Dependencies not installed**
   ```bash
   pip install -r requirements.txt
   ```

### No Signals Detected

1. **Check channel access:**
   - Ensure you're a member of TARGET_CHANNEL
   - Verify channel username is correct

2. **Check signal format:**
   - Signals must match expected format
   - View logs with `LOG_LEVEL=DEBUG`

3. **Check Telegram connection:**
   ```bash
   grep "Telegram" logs/trade_logs.log
   ```

### Orders Not Executing

1. **Check Dhan connection:**
   ```bash
   grep "Dhan" logs/trade_logs.log
   ```

2. **Verify Dhan credentials:**
   - Client ID is correct
   - Access token is valid (tokens expire!)

3. **Check error logs:**
   ```bash
   grep "ORDER REJECTED" logs/errors.log
   ```

### High Memory Usage

1. **Check CSV cache:**
   ```bash
   du -sh cache/
   ```

2. **Clear old logs:**
   ```bash
   find logs/ -name "*.log.*" -mtime +7 -delete
   ```

### Telegram Session Issues

If you get "Could not stabilize connection":

```bash
# Remove old session
rm telegram_session.session*

# Restart bot (will prompt for phone number)
./start_bot.sh restart
```

---

## Maintenance

### Daily Tasks

- Check bot status: `./start_bot.sh status`
- Review error logs: `tail logs/errors.log`
- Run health check: `python monitor.py`

### Weekly Tasks

- Review trade logs for anomalies
- Check disk space: `df -h`
- Update access tokens if needed

### Monthly Tasks

- Update dependencies:
  ```bash
  source .venv/bin/activate
  pip install --upgrade -r requirements.txt
  ```

- Clean old logs:
  ```bash
  find logs/ -name "*.log.*" -mtime +30 -delete
  ```

### Updating the Bot

```bash
# Stop the bot
./start_bot.sh stop

# Pull latest changes
git pull

# Install any new dependencies
source .venv/bin/activate
pip install -r requirements.txt

# Start the bot
./start_bot.sh start
```

### Backup Configuration

```bash
# Backup .env file
cp .env .env.backup

# Backup signals
cp signals.json backups/signals_$(date +%Y%m%d).json
```

---

## Security Considerations

1. **Never commit .env file**
   - It's in `.gitignore` by default
   - Contains sensitive credentials

2. **Protect access tokens**
   - Dhan tokens expire - rotate regularly
   - Don't share tokens

3. **Secure your server**
   ```bash
   # Enable firewall
   sudo ufw enable

   # Allow SSH
   sudo ufw allow ssh
   ```

4. **Regular updates**
   ```bash
   sudo apt update && sudo apt upgrade -y
   ```

---

## Performance Optimization

### For Low-Memory Systems

Edit `.env`:
```env
LOG_LEVEL=WARNING
MAX_LOG_SIZE_MB=10
LOG_BACKUP_COUNT=2
```

### For High-Frequency Trading

Edit `main.py`:
```python
BATCH_DELAY_SECONDS = 1.0  # Faster signal processing
```

---

## Cron Jobs (Optional)

Add automated tasks:

```bash
crontab -e
```

Add these lines:

```cron
# Health check every hour
0 * * * * cd /home/user/trading-bot && python monitor.py >> logs/health.log 2>&1

# Cleanup old logs daily at 2 AM
0 2 * * * find /home/user/trading-bot/logs -name "*.log.*" -mtime +7 -delete

# Restart bot daily at 3 AM (optional)
0 3 * * * /home/user/trading-bot/start_bot.sh restart
```

---

## Support

For issues or questions:

1. Check logs: `logs/errors.log`
2. Run health check: `python monitor.py`
3. Check GitHub issues
4. Review this documentation

---

## Quick Reference

```bash
# Start bot
./start_bot.sh start

# Stop bot
./start_bot.sh stop

# Check status
./start_bot.sh status

# View logs
./start_bot.sh logs

# Health check
python monitor.py

# Update credentials
nano .env
```

---

**Happy Trading! 📈**
