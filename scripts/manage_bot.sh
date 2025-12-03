#!/bin/bash

# Trading Bot Management Script (Systemd Version)
# Easy interface for managing the trading bot

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Check if running as root
check_root() {
    if [ "$EUID" -ne 0 ]; then
        echo -e "${RED}❌ Please run with sudo${NC}"
        exit 1
    fi
}

# Display usage
usage() {
    echo ""
    echo "Trading Bot Management"
    echo "======================"
    echo ""
    echo "Usage: sudo $0 {start|stop|restart|status|logs|timers|enable|disable|csv}"
    echo ""
    echo "Commands:"
    echo "  start     - Start the trading bot now"
    echo "  stop      - Stop the trading bot now"
    echo "  restart   - Restart the trading bot"
    echo "  status    - Check bot status"
    echo "  logs      - View real-time logs"
    echo "  timers    - Show scheduled timers"
    echo "  enable    - Enable automatic scheduling"
    echo "  disable   - Disable automatic scheduling"
    echo "  csv       - Manually download Dhan CSV"
    echo ""
}

# Start bot
start_bot() {
    check_root
    echo -e "${BLUE}▶️  Starting trading bot...${NC}"
    systemctl start trading-bot.service
    sleep 2
    if systemctl is-active --quiet trading-bot.service; then
        echo -e "${GREEN}✅ Trading bot started successfully${NC}"
        systemctl status trading-bot.service --no-pager -l
    else
        echo -e "${RED}❌ Failed to start trading bot${NC}"
        journalctl -u trading-bot -n 20 --no-pager
        exit 1
    fi
}

# Stop bot
stop_bot() {
    check_root
    echo -e "${YELLOW}⏹️  Stopping trading bot...${NC}"
    systemctl stop trading-bot.service
    sleep 2
    if ! systemctl is-active --quiet trading-bot.service; then
        echo -e "${GREEN}✅ Trading bot stopped${NC}"
    else
        echo -e "${RED}❌ Failed to stop trading bot${NC}"
        exit 1
    fi
}

# Restart bot
restart_bot() {
    check_root
    echo -e "${BLUE}🔄 Restarting trading bot...${NC}"
    systemctl restart trading-bot.service
    sleep 2
    if systemctl is-active --quiet trading-bot.service; then
        echo -e "${GREEN}✅ Trading bot restarted successfully${NC}"
        systemctl status trading-bot.service --no-pager -l
    else
        echo -e "${RED}❌ Failed to restart trading bot${NC}"
        journalctl -u trading-bot -n 20 --no-pager
        exit 1
    fi
}

# Check status
check_status() {
    echo -e "${BLUE}📊 Trading Bot Status${NC}"
    echo ""
    systemctl status trading-bot.service --no-pager -l || true
    echo ""
    echo -e "${BLUE}⏰ Scheduled Timers${NC}"
    systemctl list-timers trading-bot-* --no-pager || true
}

# View logs
view_logs() {
    echo -e "${BLUE}📜 Trading Bot Logs (Press Ctrl+C to exit)${NC}"
    echo ""
    journalctl -u trading-bot -f
}

# Show timers
show_timers() {
    echo -e "${BLUE}⏰ Scheduled Timers${NC}"
    echo ""
    systemctl list-timers --no-pager
    echo ""
    echo -e "${BLUE}📋 Trading Bot Timers Status${NC}"
    echo ""
    for timer in trading-bot-start.timer trading-bot-stop.timer trading-bot-csv-download.timer; do
        if systemctl is-enabled --quiet "$timer" 2>/dev/null; then
            echo -e "  ${GREEN}✅${NC} $timer - enabled"
        else
            echo -e "  ${RED}❌${NC} $timer - disabled"
        fi
    done
}

# Enable automatic scheduling
enable_scheduling() {
    check_root
    echo -e "${BLUE}⚡ Enabling automatic scheduling...${NC}"
    systemctl enable trading-bot-start.timer
    systemctl enable trading-bot-stop.timer
    systemctl enable trading-bot-csv-download.timer
    systemctl start trading-bot-start.timer
    systemctl start trading-bot-stop.timer
    systemctl start trading-bot-csv-download.timer
    echo -e "${GREEN}✅ Automatic scheduling enabled${NC}"
    show_timers
}

# Disable automatic scheduling
disable_scheduling() {
    check_root
    echo -e "${YELLOW}⏸️  Disabling automatic scheduling...${NC}"
    systemctl stop trading-bot-start.timer
    systemctl stop trading-bot-stop.timer
    systemctl stop trading-bot-csv-download.timer
    systemctl disable trading-bot-start.timer
    systemctl disable trading-bot-stop.timer
    systemctl disable trading-bot-csv-download.timer
    echo -e "${GREEN}✅ Automatic scheduling disabled${NC}"
}

# Download CSV manually
download_csv() {
    check_root
    echo -e "${BLUE}📥 Downloading Dhan CSV...${NC}"
    systemctl start trading-bot-csv-download.service
    sleep 2
    echo -e "${GREEN}✅ CSV download triggered${NC}"
    echo ""
    echo "View logs with: journalctl -u trading-bot-csv-download -n 50"
}

# Main script
case "${1:-}" in
    start)
        start_bot
        ;;
    stop)
        stop_bot
        ;;
    restart)
        restart_bot
        ;;
    status)
        check_status
        ;;
    logs)
        view_logs
        ;;
    timers)
        show_timers
        ;;
    enable)
        enable_scheduling
        ;;
    disable)
        disable_scheduling
        ;;
    csv)
        download_csv
        ;;
    *)
        usage
        exit 1
        ;;
esac
