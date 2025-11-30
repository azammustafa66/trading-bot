# Trading Bot - Automated Options Trading System

An intelligent trading bot that listens to Telegram channels for trading signals and automatically executes options trades on Dhan using Super Orders.

## 🎯 Overview

This bot automates the entire trading workflow:
1. **Listens** to Telegram channels for trading signals
2. **Parses** signal messages to extract trade details
3. **Maps** trading symbols to Dhan security IDs
4. **Executes** Super Orders (entry + target + stop loss) on Dhan
5. **Monitors** positions with trailing stop loss

## ✨ Key Features

- **Automated Signal Processing**: Batches multi-part Telegram messages (2-second delay)
- **Smart Deduplication**: Prevents duplicate trades (60-minute window)
- **Risk Management**: Position sizing based on stop loss (₹3,500 intraday, ₹5,000 positional)
- **Super Orders**: All-in-one orders with entry, target (10x), and stop loss
- **Breakout Detection**: Switches to MARKET order if price crosses trigger
- **Price Protection**: Skips orders if price moved >3% to avoid slippage
- **Scheduled Execution**: Auto-start at 9 AM, auto-stop at 3:35 PM (Mon-Fri)
- **Crash Recovery**: Systemd service + cron monitoring every 15 minutes
- **CSV Pre-download**: Fetches Dhan master CSV at 8:50 AM (before market opens)
- **Comprehensive Logging**: Rotating file logs with separate error tracking

## 📁 Project Structure

```
trading-bot/
├── main.py                      # Entry point - Telegram listener & signal batcher
├── core/
│   ├── signal_parser.py         # Parses Telegram messages into structured signals
│   ├── dhan_mapper.py           # Maps trading symbols to Dhan security IDs
│   └── dhan_bridge.py           # Executes Super Orders via Dhan API
├── utils/
│   └── monitor.py               # Health check monitoring
├── scripts/
│   ├── pre_download_csv.sh      # Pre-downloads Dhan CSV at 8:50 AM
│   ├── setup_schedule.sh        # Sets up all cron jobs
│   └── update_cron_paths.sh     # Updates cron job paths
├── start_bot.sh                 # Bot management (start/stop/restart/status/logs)
├── data/
│   └── signals.jsonl            # Stored trading signals (JSONL format)
├── cache/
│   └── dhan_master.csv          # Cached Dhan security master (500MB)
├── logs/
│   ├── trade_logs.log           # Main trading logs (rotating, 10MB max)
│   └── errors.log               # Error-only logs
├── requirements.txt             # Python dependencies
├── .env.example                 # Environment variables template
└── trading-bot.service          # Systemd service file

```

## 🔄 Signal Processing Pipeline

```
┌─────────────────┐
│  Telegram       │  User sends trading signal in channel
│  Channel        │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  main.py        │  Listens & batches messages (2s delay for multi-part)
│  (Listener)     │  Deduplicates signals (60-min window)
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  signal_parser  │  Extracts: symbol, strike, expiry, action, prices
│  .py            │  Validates signal format
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  dhan_mapper    │  Looks up security ID from Dhan CSV
│  .py            │  Returns: security_id, exchange, lot_size
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  dhan_bridge    │  1. Fetches current LTP
│  .py            │  2. Calculates quantity (risk-based)
│                 │  3. Determines MARKET vs LIMIT
│                 │  4. Executes Super Order (entry+target+SL)
└─────────────────┘
         │
         ▼
┌─────────────────┐
│  Dhan API       │  Order placed on exchange
│  (Super Order)  │  Entry → Target (10x) → Stop Loss → Trailing
└─────────────────┘
```

## 🚀 Installation

### Prerequisites
- Ubuntu/Linux server
- Python 3.9+
- Dhan trading account with API access
- Telegram account

### Step 1: Clone Repository
```bash
cd /opt
git clone <your-repo-url> trading_bot
cd trading_bot
```

### Step 2: Create Virtual Environment
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### Step 3: Configure Environment Variables
```bash
cp .env.example .env
nano .env
```

Fill in your credentials:
```env
# Telegram API (get from https://my.telegram.org)
TELEGRAM_API_ID=your_api_id
TELEGRAM_API_HASH=your_api_hash
TELEGRAM_PHONE=+919876543210

# Telegram Channel (ID, username, or display name)
TELEGRAM_CHANNEL=-1001234567890

# Dhan API (get from https://dhan.co)
DHAN_CLIENT_ID=your_client_id
DHAN_ACCESS_TOKEN=your_access_token

# CSV Update (daily at this hour)
CSV_UPDATE_HOUR=8
```

### Step 4: First Login (One-time Telegram Authentication)
```bash
./start_bot.sh start
# Enter OTP when prompted
./start_bot.sh stop
```

### Step 5: Set Up Automated Scheduling
```bash
cd scripts
chmod +x *.sh
./setup_schedule.sh
```

This creates:
- **8:50 AM**: Pre-download Dhan CSV
- **9:00 AM**: Start bot
- **Every 15 min (9-15)**: Crash recovery check
- **3:35 PM**: Stop bot
- **11:59 PM Sun**: Log cleanup

### Step 6: Set Up Systemd Service (Optional - for crash recovery)
```bash
sudo cp trading-bot.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable trading-bot.service
```

## 📊 Usage

### Manual Control
```bash
./start_bot.sh start       # Start the bot
./start_bot.sh stop        # Stop the bot
./start_bot.sh restart     # Restart the bot
./start_bot.sh status      # Check if running
./start_bot.sh logs        # View recent logs
./start_bot.sh cleanup     # Clean old logs
```

### Monitoring Logs
```bash
# Real-time log monitoring
tail -f logs/trade_logs.log

# View errors only
tail -f logs/errors.log

# Search for specific symbol
grep "NIFTY" logs/trade_logs.log

# Check recent orders
grep "ORDER PLACED" logs/trade_logs.log
```

### Checking Cron Jobs
```bash
crontab -l                 # View all scheduled jobs
```

## ⚙️ Configuration

### Risk Parameters (dhan_bridge.py)
```python
self.RISK_INTRADAY = 3500      # ₹3,500 per intraday trade
self.RISK_POSITIONAL = 5000    # ₹5,000 per positional trade
```

### Strategy Parameters (dhan_bridge.py)
```python
target_price = entry_price * 10.0     # 10x target (ride the trend)
sl_price = entry_price * 0.90         # 10% stop loss (if not in signal)
trailing_jump = entry_price * 0.05    # 5% trailing stop loss
```

### Order Type Logic (dhan_bridge.py)
```python
# MARKET order if:
# - Current LTP >= entry price (breakout)
# - Price hasn't moved >3% (avoid slippage)

# LIMIT order if:
# - Current LTP < entry price (waiting for trigger)
```

### Product Type Mapping (dhan_bridge.py)
```python
is_positional = False  →  productType = "INTRADAY"
is_positional = True   →  productType = "MARGIN"
```

## 🔍 How It Works

### 1. Signal Detection
- Bot connects to Telegram using `telethon`
- Listens to specified channel via `TELEGRAM_CHANNEL`
- Batches messages with 2-second delay (for multi-part signals)

### 2. Signal Parsing
- Extracts: underlying, strike, expiry, action (BUY/SELL), trigger, target, stop loss
- Example signal format:
  ```
  NIFTY 24500 CE
  Expiry: 27 DEC
  Action: BUY above 125
  Target: 150
  Stop Loss: 110
  ```

### 3. Security ID Lookup
- Downloads 500MB Dhan CSV daily at 8:50 AM
- Caches in `cache/dhan_master.csv`
- Uses `polars` for fast CSV scanning
- Maps `NIFTY24500CE` → Security ID + Exchange + Lot Size

### 4. Order Execution
- Fetches current LTP from Dhan API
- Calculates quantity: `risk_capital / (entry - stop_loss)`
- Rounds to lot size multiples
- Places Super Order with:
  - Entry: MARKET or LIMIT based on LTP
  - Target: 10x entry price
  - Stop Loss: From signal or 10% below entry
  - Trailing: 5% jump on profit

### 5. Order Types
**Super Order** = Entry Leg + Target Leg + Stop Loss Leg

When entry executes:
- Target order placed at 10x price
- Stop loss order placed
- Trailing stop loss activated (5% jump)

## 🛠️ Troubleshooting

### Bot Not Starting
```bash
# Check logs
./start_bot.sh logs

# Check if already running
./start_bot.sh status

# Force cleanup and restart
./start_bot.sh cleanup
./start_bot.sh restart
```

### "Cannot find entity" Error
- Update `TELEGRAM_CHANNEL` in `.env`
- Try channel ID (numeric), username (@channel), or exact display name
- Check channel resolution in logs

### CSV Download Issues
```bash
# Manually trigger CSV download
source .venv/bin/activate
python -c "from core.dhan_mapper import DhanMapper; DhanMapper().download_csv()"
```

### Orders Not Executing
1. Check Dhan credentials in `.env`
2. Verify `DHAN_ACCESS_TOKEN` is valid (expires daily)
3. Check logs for API errors: `grep "ERROR" logs/errors.log`
4. Verify signal parsing: `grep "Signal Details" logs/trade_logs.log`

### Cron Jobs Not Running
```bash
# Check cron service
sudo systemctl status cron

# View cron logs
grep CRON /var/log/syslog

# Re-setup schedule
cd scripts
./setup_schedule.sh
```

## 📈 Performance & Monitoring

### Log Rotation
- Main log: `logs/trade_logs.log` (10MB max, 5 backups)
- Error log: `logs/errors.log` (5MB max, 3 backups)
- Auto-cleanup: Every Sunday at 11:59 PM (keeps last 7 days)

### Health Monitoring
- Cron checks every 15 minutes (9 AM - 3 PM)
- Systemd restarts on crash (if enabled)
- Monitor script: `utils/monitor.py`

### Signal Storage
- All signals saved to `data/signals.jsonl`
- One signal per line (JSON format)
- Includes timestamp, symbol, prices, execution status

## 🔐 Security

- Never commit `.env` file (contains API keys)
- Rotate Dhan access token daily
- Use secure server with SSH key auth
- Restrict file permissions: `chmod 600 .env`

## 📝 API Documentation

- **Dhan Super Order API**: https://dhanhq.co/docs/v2/super-order/
- **Telegram API**: https://core.telegram.org/api

## 🤝 Contributing

This is a personal trading bot. For issues or suggestions, please create an issue in the repository.

## ⚠️ Disclaimer

**Trading involves risk. This bot is for educational purposes only. Use at your own risk.**

- Past performance doesn't guarantee future results
- Always test with paper trading first
- Never invest more than you can afford to lose
- Ensure you understand options trading before using this bot
- The author is not responsible for any financial losses

## 📧 Support

For issues:
1. Check logs: `./start_bot.sh logs`
2. Review troubleshooting section above
3. Create an issue in the repository with logs

---

**Built with ❤️ for automated options trading**
