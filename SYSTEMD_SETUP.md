# Systemd Setup Guide

This guide explains how to set up the trading bot with systemd for reliable, SSH-independent operation.

## Why Systemd?

✅ **Advantages over Cron + Screen:**
- Runs independently of SSH sessions
- Automatic crash recovery
- Better logging with journalctl
- More reliable scheduling
- Easier management and monitoring
- Process isolation and resource limits

## Architecture

### Services
1. **trading-bot.service** - Main bot process
   - Runs the Python bot
   - Auto-restarts on crashes (during market hours)
   - Logs to systemd journal

2. **trading-bot-csv-download.service** - CSV pre-download
   - Downloads Dhan master CSV
   - Triggered by timer at 8:50 AM

3. **trading-bot-start.service** - Start trigger
   - Starts the main bot
   - Triggered by timer at 9:00 AM

4. **trading-bot-stop.service** - Stop trigger
   - Stops the main bot
   - Triggered by timer at 3:35 PM

### Timers
1. **trading-bot-start.timer** - Starts bot at 9:00 AM (Mon-Fri)
2. **trading-bot-stop.timer** - Stops bot at 3:35 PM (Mon-Fri)
3. **trading-bot-csv-download.timer** - Downloads CSV at 8:50 AM (Mon-Fri)

## Installation

### Step 1: Run Setup Script (One-time)

On your **droplet** (not local machine), run:

```bash
cd /opt/trading_bot
sudo ./scripts/setup_systemd.sh
```

This will:
- Copy all service and timer files to `/etc/systemd/system/`
- Enable and start all timers
- Optionally remove conflicting cron jobs
- Reload systemd daemon

### Step 2: Verify Installation

Check that timers are active:

```bash
systemctl list-timers trading-bot-*
```

You should see:
```
NEXT                         LEFT          LAST  PASSED  UNIT                           ACTIVATES
Mon 2025-12-02 08:50:00 IST  12h left      -     -       trading-bot-csv-download.timer trading-bot-csv-download.service
Mon 2025-12-02 09:00:00 IST  12h left      -     -       trading-bot-start.timer        trading-bot-start.service
Mon 2025-12-02 15:35:00 IST  18h left      -     -       trading-bot-stop.timer         trading-bot-stop.service
```

## Daily Operation

### Automatic Schedule (No Action Needed)

The bot runs automatically:
1. **8:50 AM** - Downloads fresh Dhan CSV
2. **9:00 AM** - Bot starts listening to Telegram
3. **During market hours** - Auto-restarts if crashes
4. **3:35 PM** - Bot stops gracefully

### Manual Control

Use the management script for manual control:

```bash
# Start bot manually
sudo /opt/trading_bot/scripts/manage_bot.sh start

# Stop bot manually
sudo /opt/trading_bot/scripts/manage_bot.sh stop

# Restart bot
sudo /opt/trading_bot/scripts/manage_bot.sh restart

# Check status
sudo /opt/trading_bot/scripts/manage_bot.sh status

# View live logs
sudo /opt/trading_bot/scripts/manage_bot.sh logs

# Check scheduled timers
sudo /opt/trading_bot/scripts/manage_bot.sh timers

# Download CSV manually
sudo /opt/trading_bot/scripts/manage_bot.sh csv
```

## Monitoring

### Check Bot Status

```bash
systemctl status trading-bot
```

### View Logs

**Real-time logs:**
```bash
journalctl -u trading-bot -f
```

**Last 100 lines:**
```bash
journalctl -u trading-bot -n 100
```

**Logs from today:**
```bash
journalctl -u trading-bot --since today
```

**Logs with errors only:**
```bash
journalctl -u trading-bot -p err
```

**CSV download logs:**
```bash
journalctl -u trading-bot-csv-download -n 50
```

### Check Timers

**List all trading bot timers:**
```bash
systemctl list-timers trading-bot-*
```

**Check specific timer:**
```bash
systemctl status trading-bot-start.timer
```

## Troubleshooting

### Bot Not Starting Automatically

1. **Check timer status:**
   ```bash
   systemctl status trading-bot-start.timer
   ```

2. **Ensure timer is enabled:**
   ```bash
   sudo systemctl enable trading-bot-start.timer
   sudo systemctl start trading-bot-start.timer
   ```

3. **Check logs:**
   ```bash
   journalctl -u trading-bot-start -n 20
   ```

### Bot Stops After SSH Disconnect

✅ **This should NOT happen with systemd!**

If it does:
1. Check that you're using the systemd service, not screen/tmux
2. Verify service is enabled: `systemctl is-enabled trading-bot`
3. Check service type: `systemctl cat trading-bot | grep Type`
   - Should show: `Type=simple`

### Bot Not Restarting After Crash

1. **Check restart policy:**
   ```bash
   systemctl cat trading-bot | grep Restart
   ```
   Should show: `Restart=on-failure`

2. **Check if restart limit reached:**
   ```bash
   systemctl status trading-bot
   ```
   Look for "start request repeated too quickly"

3. **Reset failed state:**
   ```bash
   sudo systemctl reset-failed trading-bot
   sudo systemctl start trading-bot
   ```

### Wrong Time Zone

1. **Check system timezone:**
   ```bash
   timedatectl
   ```

2. **Set to IST if needed:**
   ```bash
   sudo timedatectl set-timezone Asia/Kolkata
   ```

3. **Restart timers:**
   ```bash
   sudo systemctl restart trading-bot-start.timer
   sudo systemctl restart trading-bot-stop.timer
   sudo systemctl restart trading-bot-csv-download.timer
   ```

### CSV Download Failing

1. **Check CSV download logs:**
   ```bash
   journalctl -u trading-bot-csv-download -n 50
   ```

2. **Test manually:**
   ```bash
   sudo /opt/trading_bot/scripts/manage_bot.sh csv
   journalctl -u trading-bot-csv-download -f
   ```

3. **Check network connectivity:**
   ```bash
   curl -I https://images.dhan.co/api-data/api-scrip-master.csv
   ```

## Advanced Configuration

### Change Schedule Times

Edit timer files in `/etc/systemd/system/`:

```bash
sudo nano /etc/systemd/system/trading-bot-start.timer
```

Change the time:
```ini
OnCalendar=Mon-Fri 09:00:00  # Change 09:00:00 to desired time
```

Reload and restart:
```bash
sudo systemctl daemon-reload
sudo systemctl restart trading-bot-start.timer
```

### Disable Auto-Restart on Crash

If you don't want auto-restart:

```bash
sudo nano /etc/systemd/system/trading-bot.service
```

Change:
```ini
Restart=on-failure  →  Restart=no
```

Reload:
```bash
sudo systemctl daemon-reload
```

### Enable Bot to Start on Boot (Always Running)

⚠️ **Not recommended** - bot should only run during market hours.

But if needed:
```bash
sudo systemctl enable trading-bot
```

### View Resource Usage

```bash
systemctl status trading-bot
```

Look for:
- Memory usage
- CPU usage
- Process uptime

## File Locations

- **Service files:** `/etc/systemd/system/trading-bot*.service`
- **Timer files:** `/etc/systemd/system/trading-bot*.timer`
- **Logs:** `journalctl -u trading-bot` (systemd journal)
- **Bot logs:** `/opt/trading_bot/logs/` (application logs)
- **Management script:** `/opt/trading_bot/scripts/manage_bot.sh`

## Removing Systemd Setup

To go back to cron-based setup:

```bash
# Stop and disable all services
sudo systemctl stop trading-bot-start.timer
sudo systemctl stop trading-bot-stop.timer
sudo systemctl stop trading-bot-csv-download.timer
sudo systemctl disable trading-bot-start.timer
sudo systemctl disable trading-bot-stop.timer
sudo systemctl disable trading-bot-csv-download.timer
sudo systemctl disable trading-bot

# Remove service files
sudo rm /etc/systemd/system/trading-bot*

# Reload systemd
sudo systemctl daemon-reload

# Re-setup cron
cd /opt/trading_bot/scripts
./setup_schedule.sh
```

## Quick Reference

| Task | Command |
|------|---------|
| Check if bot is running | `systemctl status trading-bot` |
| Start bot now | `sudo systemctl start trading-bot` |
| Stop bot now | `sudo systemctl stop trading-bot` |
| View live logs | `journalctl -u trading-bot -f` |
| Check schedule | `systemctl list-timers trading-bot-*` |
| Enable auto-schedule | `sudo systemctl enable trading-bot-*.timer` |
| Disable auto-schedule | `sudo systemctl disable trading-bot-*.timer` |
| Reload after config change | `sudo systemctl daemon-reload` |

---

**✅ With systemd, your bot will run reliably even if you close your terminal or lose SSH connection!**
