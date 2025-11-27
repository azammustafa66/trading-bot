# Trading Bot - Automatic Scheduling Guide

This guide explains how to set up automatic start/stop scheduling for the trading bot.

## 🎯 Features

1. **Auto-start at 9:00 AM** (Monday-Friday)
2. **Auto-stop at 3:35 PM** (Monday-Friday)
3. **Pre-download CSV at 8:50 AM** (before market opens)
4. **Auto-restart on crash** (via systemd)
5. **Health checks every hour** during trading hours
6. **Automatic log cleanup** (weekly)

## ⚡ Quick Setup

```bash
# Make the script executable
chmod +x setup_schedule.sh

# Run the setup
./setup_schedule.sh
```

That's it! The bot will now run automatically on weekdays.

## 📅 Default Schedule

| Time | Action | Days | Description |
|------|--------|------|-------------|
| 8:50 AM | Pre-download CSV | Mon-Fri | Downloads Dhan master CSV before market |
| 9:00 AM | Start Bot | Mon-Fri | Bot starts 15 min before market opens |
| Every hour | Health Check | Mon-Fri | Monitors bot health (9 AM - 4 PM) |
| 3:35 PM | Stop Bot | Mon-Fri | Bot stops 5 min after market closes |
| 11:59 PM | Cleanup Logs | Sunday | Removes logs older than 7 days |

## 🔄 How It Works

### 1. Cron Jobs (Scheduling)
Cron handles the time-based scheduling:
- Starts/stops bot at specific times
- Only runs on trading days (Mon-Fri)
- Pre-downloads CSV to avoid delays

### 2. Systemd (Crash Recovery)
Systemd handles crash recovery:
- If bot crashes, it auto-restarts after 10 seconds
- Unlimited restart attempts during trading hours
- Logs all crashes to systemd journal

### 3. Combined Behavior

**Normal Day (No crashes):**
```
8:50 AM  → CSV downloads
9:00 AM  → Bot starts
9:00 AM-3:35 PM → Bot runs
3:35 PM  → Bot stops
```

**Day with Crash:**
```
8:50 AM  → CSV downloads
9:00 AM  → Bot starts
10:30 AM → Bot crashes
10:30 AM → Systemd auto-restarts (10 sec delay)
10:31 AM → Bot running again
3:35 PM  → Bot stops
```

## 📊 Why Pre-download CSV at 8:50 AM?

The Dhan master CSV:
- Is ~500MB in size
- Takes 2-5 minutes to download
- Updates daily
- Is needed to map trading symbols

By downloading at 8:50 AM:
- CSV is ready before market opens (9:15 AM)
- No delay when first signal arrives
- Bot responds faster to signals

## 🛠️ Manual Override

The scheduled tasks don't prevent manual control:

```bash
# Manual start (even outside trading hours)
./start_bot.sh start

# Manual stop
./start_bot.sh stop

# Check status
./start_bot.sh status

# View logs
./start_bot.sh logs
```

## 📝 View Scheduled Tasks

```bash
# List all cron jobs
crontab -l

# View cron execution logs
tail -f logs/cron.log

# View health check logs
tail -f logs/health.log
```

## 🔧 Customizing the Schedule

### Change Start/Stop Times

Edit the crontab:
```bash
crontab -e
```

Find these lines:
```cron
# Start at 9:00 AM
0 9 * * 1-5 /root/trading-bot/start_bot.sh start

# Stop at 3:35 PM
35 15 * * 1-5 /root/trading-bot/start_bot.sh stop
```

Change the time:
- First number: Minutes (0-59)
- Second number: Hours (0-23, in 24-hour format)

Examples:
```cron
# Start at 9:15 AM
15 9 * * 1-5 /root/trading-bot/start_bot.sh start

# Stop at 4:00 PM
0 16 * * 1-5 /root/trading-bot/start_bot.sh stop
```

### Change CSV Download Time

```cron
# Download at 8:30 AM instead of 8:50 AM
30 8 * * 1-5 /root/trading-bot/pre_download_csv.sh
```

### Add Specific Holidays

To skip certain dates (like market holidays):

```bash
crontab -e
```

Add conditions to skip specific dates:
```cron
# Don't run on Jan 26 (Republic Day)
0 9 * * 1-5 [ $(date +\%m-\%d) != "01-26" ] && /root/trading-bot/start_bot.sh start
```

## 🔍 Monitoring

### Check if Bot is Running
```bash
./start_bot.sh status
```

### Full Health Check
```bash
python monitor.py
```

### View Cron Logs
```bash
tail -f logs/cron.log
```

### View Systemd Restart History
```bash
# If using systemd service
systemctl status trading-bot

# View restart logs
journalctl -u trading-bot | grep -i restart
```

## 🚨 Troubleshooting

### Bot Didn't Start at 9 AM

**Check cron logs:**
```bash
tail -20 logs/cron.log
```

**Check if cron is running:**
```bash
systemctl status cron
```

**Verify crontab entries:**
```bash
crontab -l | grep trading-bot
```

### Bot Keeps Crashing

**Check error logs:**
```bash
tail -50 logs/errors.log
```

**Check systemd logs:**
```bash
journalctl -u trading-bot -f
```

**Check restart count:**
```bash
systemctl status trading-bot
```

### CSV Download Failed

**Check cron logs:**
```bash
grep "CSV" logs/cron.log
```

**Manually test download:**
```bash
./pre_download_csv.sh
```

**Check network connectivity:**
```bash
ping -c 3 images.dhan.co
```

## 🔐 Security Notes

### Cron Email Notifications

By default, cron sends email on errors. To disable:
```bash
crontab -e
```

Add at the top:
```cron
MAILTO=""
```

Or redirect to a log file (already done in setup_schedule.sh).

### Systemd Security

The service runs with:
- Limited memory (1GB max)
- Limited CPU (50% max)
- Isolated tmp directory
- Read-only system files

## 📊 Logs Location

All scheduling logs are in:
```
logs/
├── cron.log          # Cron execution logs
├── health.log        # Health check logs
├── trade_logs.log    # Main bot logs
└── errors.log        # Error logs
```

## 🔄 Disable Automatic Scheduling

To remove automatic scheduling:

```bash
# Edit crontab
crontab -e

# Remove all lines containing "trading-bot"
# Or delete the entire TRADING BOT SCHEDULE section

# Save and exit
```

To disable systemd service:
```bash
systemctl disable trading-bot
systemctl stop trading-bot
```

## ⚙️ Advanced Configuration

### Run on Weekends Too

Change `1-5` (Mon-Fri) to `0-6` (Sun-Sat):
```cron
0 9 * * 0-6 /root/trading-bot/start_bot.sh start
```

### Multiple Sessions Per Day

```cron
# Morning session: 9 AM - 12 PM
0 9 * * 1-5 /root/trading-bot/start_bot.sh start
0 12 * * 1-5 /root/trading-bot/start_bot.sh stop

# Afternoon session: 1 PM - 3:35 PM
0 13 * * 1-5 /root/trading-bot/start_bot.sh start
35 15 * * 1-5 /root/trading-bot/start_bot.sh stop
```

### Automatic Restart at Specific Time

```cron
# Restart bot at 12 PM daily (to clear any issues)
0 12 * * 1-5 /root/trading-bot/start_bot.sh restart
```

## 📈 Best Practices

1. **Monitor the first week** - Check logs daily to ensure everything works
2. **Test manually first** - Run start/stop manually before enabling cron
3. **Set up alerts** - Configure email/SMS for critical errors
4. **Keep backups** - Backup your .env and signals regularly
5. **Update regularly** - Pull latest code updates weekly

## 🎯 Quick Reference

```bash
# Setup automatic scheduling
./setup_schedule.sh

# View schedule
crontab -l

# View execution logs
tail -f logs/cron.log

# Manual control
./start_bot.sh start
./start_bot.sh stop
./start_bot.sh status

# Health check
python monitor.py

# View bot logs
tail -f logs/trade_logs.log
```

---

**Your bot is now fully automated! 🚀**

It will:
- ✅ Start every weekday at 9:00 AM
- ✅ Stop every weekday at 3:35 PM
- ✅ Auto-restart if it crashes
- ✅ Pre-download CSV before market
- ✅ Monitor its own health
- ✅ Clean up old logs

Just make sure your droplet is running 24/7!
