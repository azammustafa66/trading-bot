#!/bin/bash

# Setup Systemd Services and Timers for Trading Bot
# This script replaces cron-based scheduling with systemd

set -e

echo "=========================================="
echo "Trading Bot - Systemd Setup"
echo "=========================================="
echo ""

# Check if running as root
if [ "$EUID" -ne 0 ]; then
    echo "❌ Please run as root or with sudo"
    exit 1
fi

# Get the project directory (parent of scripts/)
PROJECT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )/.." && pwd )"
SYSTEMD_DIR="/etc/systemd/system"

echo "📁 Project Directory: $PROJECT_DIR"
echo "📁 Systemd Directory: $SYSTEMD_DIR"
echo ""

# Check if systemd files exist
if [ ! -d "$PROJECT_DIR/systemd" ]; then
    echo "❌ systemd/ directory not found in $PROJECT_DIR"
    exit 1
fi

echo "🔧 Installing systemd service files..."
echo ""

# Copy all service and timer files
cp "$PROJECT_DIR/systemd/trading-bot.service" "$SYSTEMD_DIR/"
cp "$PROJECT_DIR/systemd/trading-bot-start.service" "$SYSTEMD_DIR/"
cp "$PROJECT_DIR/systemd/trading-bot-start.timer" "$SYSTEMD_DIR/"
cp "$PROJECT_DIR/systemd/trading-bot-stop.service" "$SYSTEMD_DIR/"
cp "$PROJECT_DIR/systemd/trading-bot-stop.timer" "$SYSTEMD_DIR/"
cp "$PROJECT_DIR/systemd/trading-bot-csv-download.service" "$SYSTEMD_DIR/"
cp "$PROJECT_DIR/systemd/trading-bot-csv-download.timer" "$SYSTEMD_DIR/"

echo "✅ Copied all service and timer files"
echo ""

# Reload systemd
echo "🔄 Reloading systemd daemon..."
systemctl daemon-reload
echo "✅ Systemd reloaded"
echo ""

# Enable timers (they will start the services automatically)
echo "⚡ Enabling timers..."
systemctl enable trading-bot-start.timer
systemctl enable trading-bot-stop.timer
systemctl enable trading-bot-csv-download.timer
echo "✅ Timers enabled"
echo ""

# Start timers
echo "▶️  Starting timers..."
systemctl start trading-bot-start.timer
systemctl start trading-bot-stop.timer
systemctl start trading-bot-csv-download.timer
echo "✅ Timers started"
echo ""

# Disable and remove old cron jobs (optional)
echo "🧹 Cleaning up old cron jobs..."
if crontab -l 2>/dev/null | grep -q "trading"; then
    echo "⚠️  Found existing cron jobs. Remove them? (y/n)"
    read -r response
    if [[ "$response" =~ ^[Yy]$ ]]; then
        crontab -l 2>/dev/null | grep -v "trading" | crontab - || true
        echo "✅ Cron jobs removed"
    else
        echo "⚠️  Keeping existing cron jobs (may conflict with systemd)"
    fi
else
    echo "✅ No conflicting cron jobs found"
fi
echo ""

echo "=========================================="
echo "✅ Systemd Setup Complete!"
echo "=========================================="
echo ""
echo "📋 Installed Services:"
echo "  • trading-bot.service           - Main bot service"
echo "  • trading-bot-start.timer       - Starts at 9:00 AM (Mon-Fri)"
echo "  • trading-bot-stop.timer        - Stops at 3:35 PM (Mon-Fri)"
echo "  • trading-bot-csv-download.timer - Downloads CSV at 8:50 AM (Mon-Fri)"
echo ""
echo "🔧 Useful Commands:"
echo "  • Check bot status:         systemctl status trading-bot"
echo "  • Start bot manually:       systemctl start trading-bot"
echo "  • Stop bot manually:        systemctl stop trading-bot"
echo "  • View logs (live):         journalctl -u trading-bot -f"
echo "  • View logs (last 100):     journalctl -u trading-bot -n 100"
echo "  • Check timers:             systemctl list-timers trading-bot-*"
echo "  • Check CSV download logs:  journalctl -u trading-bot-csv-download"
echo ""
echo "⏰ Schedule Summary:"
echo "  • 8:50 AM (Mon-Fri): Download Dhan CSV"
echo "  • 9:00 AM (Mon-Fri): Start bot"
echo "  • 3:35 PM (Mon-Fri): Stop bot"
echo "  • Auto-restart on crash (during market hours)"
echo ""
echo "🎯 Next Steps:"
echo "  1. Verify timers: systemctl list-timers"
echo "  2. Test manually: systemctl start trading-bot"
echo "  3. Check logs: journalctl -u trading-bot -f"
echo ""
