# --- DYNAMIC CONFIGURATION ---
# Automatically get the directory where this script is located
PROJECT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
VENV_PYTHON="$PROJECT_DIR/.venv/bin/python"
SCREEN_NAME="trading_bot"
LOG_DIR="$PROJECT_DIR/logs"
LOG_FILE="$LOG_DIR/trade_logs.log"

# Create logs directory if it doesn't exist
mkdir -p "$LOG_DIR"

# --- FUNCTIONS ---

start_bot() {
    echo "[$(date)] 🚀 Checking Bot Status..." >> $LOG_FILE
    
    # Check if screen session exists
    if screen -list | grep -q "$SCREEN_NAME"; then
        echo "[$(date)] ✅ Bot is already running." >> $LOG_FILE
    else
        echo "[$(date)] ⚠️ Bot not running. Starting..." >> $LOG_FILE
        
        # Go to project dir to ensure relative imports work
        cd "$PROJECT_DIR"
        
        # Check if venv python exists
        if [ ! -f "$VENV_PYTHON" ]; then
            echo "[$(date)] ❌ Error: Virtualenv python not found at $VENV_PYTHON" >> $LOG_FILE
            exit 1
        fi

        # Start in detached screen session
        screen -dmS $SCREEN_NAME $VENV_PYTHON main.py
        
        echo "[$(date)] 🎉 Bot started in screen session '$SCREEN_NAME'." >> $LOG_FILE
    fi
}

stop_bot() {
    echo "[$(date)] 🛑 Stopping Bot..." >> $LOG_FILE
    
    if screen -list | grep -q "$SCREEN_NAME"; then
        screen -S $SCREEN_NAME -X quit
        echo "[$(date)] 💤 Bot stopped." >> $LOG_FILE
    else
        echo "[$(date)] ℹ️ Bot was not running." >> $LOG_FILE
    fi
}

run_cleanup() {
    echo "[$(date)] 🧹 Running End-of-Day Cleanup..." >> $LOG_FILE
    cd "$PROJECT_DIR"
    $VENV_PYTHON core/clean_up_signals.py
    echo "[$(date)] ✅ Cleanup Complete." >> $LOG_FILE
}

check_status() {
    echo "🔍 Checking Trading Bot Status..."
    echo ""

    if screen -list | grep -q "$SCREEN_NAME"; then
        echo "✅ Bot is RUNNING (screen session: $SCREEN_NAME)"
        echo ""
        echo "📊 Recent activity (last 10 log lines):"
        tail -10 "$LOG_FILE" 2>/dev/null || echo "  No logs found"
        echo ""
        echo "To view live logs: screen -r $SCREEN_NAME"
        echo "To detach: Press Ctrl+A then D"
    else
        echo "❌ Bot is NOT running"
        echo ""
        echo "To start: $0 start"
    fi
    echo ""
}

view_logs() {
    if [ -f "$LOG_FILE" ]; then
        echo "📊 Viewing logs (Ctrl+C to exit)..."
        tail -f "$LOG_FILE"
    else
        echo "❌ Log file not found: $LOG_FILE"
        exit 1
    fi
}

# --- COMMAND SWITCH ---
case "$1" in
    start)
        start_bot
        ;;
    stop)
        stop_bot
        ;;
    restart)
        stop_bot
        sleep 2
        start_bot
        ;;
    status)
        check_status
        ;;
    logs)
        view_logs
        ;;
    cleanup)
        run_cleanup
        ;;
    *)
        echo "Trading Bot Management Script"
        echo ""
        echo "Usage: $0 {start|stop|restart|status|logs|cleanup}"
        echo ""
        echo "Commands:"
        echo "  start   - Start the bot in background"
        echo "  stop    - Stop the bot"
        echo "  restart - Restart the bot"
        echo "  status  - Check if bot is running"
        echo "  logs    - View live logs"
        echo "  cleanup - Run end-of-day signal cleanup"
        exit 1
        ;;
esac

exit 0