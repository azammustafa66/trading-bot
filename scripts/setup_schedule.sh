#!/bin/bash

# ==============================================
# Trading Bot - Automatic Schedule Setup
# ==============================================
# This script sets up cron jobs to:
# 1. Start bot at 9:00 AM (market opens 9:15)
# 2. Stop bot at 3:35 PM (market closes 3:30)
# 3. Pre-download CSV at 8:50 AM
# ==============================================

set -e

# Colors
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

# Get the directory where this script is located
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
# Get the project root directory (parent of scripts/)
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

echo -e "${BLUE}================================================${NC}"
echo -e "${BLUE}  Trading Bot - Schedule Setup${NC}"
echo -e "${BLUE}================================================${NC}"
echo ""

# Check if running as root
CURRENT_USER=$(whoami)
echo -e "${BLUE}Current user: ${GREEN}$CURRENT_USER${NC}"
echo ""

# Backup existing crontab
echo "Backing up existing crontab..."
crontab -l > /tmp/crontab_backup_$(date +%Y%m%d_%H%M%S).txt 2>/dev/null || true

# Create temporary crontab file
TEMP_CRON=$(mktemp)

# Get existing crontab (if any)
crontab -l 2>/dev/null > "$TEMP_CRON" || true

# Remove any existing trading-bot entries
sed -i '/trading-bot/d' "$TEMP_CRON" 2>/dev/null || true
sed -i '/pre_download_csv/d' "$TEMP_CRON" 2>/dev/null || true

echo -e "${YELLOW}Adding new cron jobs...${NC}"
echo ""

# Add new cron jobs
cat >> "$TEMP_CRON" <<EOF

# ===== TRADING BOT SCHEDULE =====
# Pre-download CSV at 8:50 AM (before market opens)
50 8 * * 1-5 $PROJECT_DIR/scripts/pre_download_csv.sh >> $PROJECT_DIR/logs/cron.log 2>&1

# Start bot at 9:00 AM (Monday to Friday)
0 9 * * 1-5 $PROJECT_DIR/start_bot.sh start >> $PROJECT_DIR/logs/cron.log 2>&1

# Stop bot at 3:35 PM (Monday to Friday)
35 15 * * 1-5 $PROJECT_DIR/start_bot.sh stop >> $PROJECT_DIR/logs/cron.log 2>&1

# Health check every hour during trading hours (9 AM - 4 PM)
0 9-16 * * 1-5 $PROJECT_DIR/scripts/monitor.py >> $PROJECT_DIR/logs/health.log 2>&1

# Cleanup old logs at 11:59 PM every Sunday
59 23 * * 0 find $PROJECT_DIR/logs -name "*.log.*" -mtime +7 -delete
# ================================

EOF

# Install the new crontab
crontab "$TEMP_CRON"
rm "$TEMP_CRON"

echo -e "${GREEN}✓${NC} Cron jobs installed successfully!"
echo ""

# Display the schedule
echo -e "${BLUE}📅 Scheduled Tasks:${NC}"
echo ""
echo -e "  ${GREEN}8:50 AM${NC}  - Pre-download Dhan CSV (Mon-Fri)"
echo -e "  ${GREEN}9:00 AM${NC}  - Start trading bot (Mon-Fri)"
echo -e "  ${GREEN}3:35 PM${NC}  - Stop trading bot (Mon-Fri)"
echo -e "  ${GREEN}Every hour${NC} - Health check (9 AM - 4 PM, Mon-Fri)"
echo -e "  ${GREEN}Sunday 11:59 PM${NC} - Cleanup old logs"
echo ""

# Show current crontab
echo -e "${BLUE}Current crontab entries:${NC}"
echo ""
crontab -l | grep -v "^#" | grep -v "^$" || echo "No entries"
echo ""

echo -e "${YELLOW}⚠  Important Notes:${NC}"
echo "  • Bot will auto-start at 9:00 AM on weekdays"
echo "  • Bot will auto-stop at 3:35 PM on weekdays"
echo "  • CSV downloads at 8:50 AM to be ready before market"
echo "  • If bot crashes, systemd will auto-restart it"
echo "  • Check logs in: $PROJECT_DIR/logs/"
echo ""

echo -e "${BLUE}Manual control still available:${NC}"
echo "  Start:  $PROJECT_DIR/start_bot.sh start"
echo "  Stop:   $PROJECT_DIR/start_bot.sh stop"
echo "  Status: $PROJECT_DIR/start_bot.sh status"
echo ""

echo -e "${GREEN}✅ Schedule setup complete!${NC}"
echo ""
