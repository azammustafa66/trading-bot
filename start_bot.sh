# --- DYNAMIC CONFIGURATION ---
# Automatically get the directory where this script is located
PROJECT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
VENV_PYTHON="$PROJECT_DIR/.venv/bin/python"
SCREEN_NAME="trading_bot"
LOG_FILE="$PROJECT_DIR/trade_logs.log"

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
    cleanup)
        run_cleanup
        ;;
    *)
        echo "Usage: $0 {start|stop|restart|cleanup}"
        exit 1
        ;;
esac

exit 0