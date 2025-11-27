#!/bin/bash

# ==============================================
# Trading Bot - Ubuntu Deployment Script
# ==============================================

set -e  # Exit on any error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Get the directory where this script is located
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd "$SCRIPT_DIR"

echo -e "${BLUE}================================================${NC}"
echo -e "${BLUE}  Trading Bot - Ubuntu Deployment${NC}"
echo -e "${BLUE}================================================${NC}"
echo ""

# ==============================================
# 1. System Requirements Check
# ==============================================
echo -e "${YELLOW}[1/9]${NC} Checking system requirements..."

# Check if running on Ubuntu/Debian
if [ ! -f /etc/os-release ]; then
    echo -e "${RED}ERROR: Cannot detect OS. /etc/os-release not found.${NC}"
    exit 1
fi

. /etc/os-release
if [[ "$ID" != "ubuntu" && "$ID" != "debian" ]]; then
    echo -e "${YELLOW}WARNING: This script is optimized for Ubuntu/Debian. You're running: $ID${NC}"
    read -p "Continue anyway? (y/N) " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        exit 1
    fi
fi

# Check Python version
if ! command -v python3 &> /dev/null; then
    echo -e "${RED}ERROR: Python 3 is not installed.${NC}"
    echo "Install it with: sudo apt update && sudo apt install python3 python3-pip python3-venv"
    exit 1
fi

PYTHON_VERSION=$(python3 --version | cut -d' ' -f2 | cut -d'.' -f1,2)
echo -e "${GREEN}✓${NC} Python version: $(python3 --version)"

if (( $(echo "$PYTHON_VERSION < 3.9" | bc -l) )); then
    echo -e "${RED}ERROR: Python 3.9+ required. Current: $PYTHON_VERSION${NC}"
    exit 1
fi

# ==============================================
# 2. Install System Dependencies
# ==============================================
echo ""
echo -e "${YELLOW}[2/9]${NC} Installing system dependencies..."

# Check if running as root
if [[ $EUID -eq 0 ]]; then
   echo -e "${YELLOW}WARNING: Running as root. It's recommended to run as a regular user.${NC}"
fi

# Update package list
echo "Updating package list..."
sudo apt update -qq

# Install required packages
echo "Installing required packages..."
sudo apt install -y -qq \
    python3-pip \
    python3-venv \
    screen \
    git \
    curl \
    bc \
    || { echo -e "${RED}ERROR: Failed to install system packages${NC}"; exit 1; }

echo -e "${GREEN}✓${NC} System dependencies installed"

# ==============================================
# 3. Create Virtual Environment
# ==============================================
echo ""
echo -e "${YELLOW}[3/9]${NC} Setting up Python virtual environment..."

if [ -d ".venv" ]; then
    echo -e "${YELLOW}Virtual environment already exists. Recreating...${NC}"
    rm -rf .venv
fi

python3 -m venv .venv || { echo -e "${RED}ERROR: Failed to create virtual environment${NC}"; exit 1; }
source .venv/bin/activate

echo -e "${GREEN}✓${NC} Virtual environment created"

# ==============================================
# 4. Install Python Dependencies
# ==============================================
echo ""
echo -e "${YELLOW}[4/9]${NC} Installing Python dependencies..."

# Upgrade pip
.venv/bin/pip install --upgrade pip setuptools wheel -q

# Install requirements
if [ ! -f "requirements.txt" ]; then
    echo -e "${RED}ERROR: requirements.txt not found!${NC}"
    exit 1
fi

.venv/bin/pip install -r requirements.txt || { echo -e "${RED}ERROR: Failed to install Python packages${NC}"; exit 1; }

echo -e "${GREEN}✓${NC} Python dependencies installed"

# ==============================================
# 5. Create Directories
# ==============================================
echo ""
echo -e "${YELLOW}[5/9]${NC} Creating necessary directories..."

mkdir -p logs
mkdir -p cache
mkdir -p backups

echo -e "${GREEN}✓${NC} Directories created"

# ==============================================
# 6. Environment Configuration
# ==============================================
echo ""
echo -e "${YELLOW}[6/9]${NC} Configuring environment..."

if [ ! -f ".env" ]; then
    if [ -f ".env.example" ]; then
        echo -e "${YELLOW}No .env file found. Creating from .env.example...${NC}"
        cp .env.example .env
        echo -e "${YELLOW}⚠ IMPORTANT: Edit .env file with your credentials!${NC}"
        echo -e "${YELLOW}   Required fields:${NC}"
        echo -e "${YELLOW}   - TELEGRAM_API_ID${NC}"
        echo -e "${YELLOW}   - TELEGRAM_API_HASH${NC}"
        echo -e "${YELLOW}   - TARGET_CHANNEL${NC}"
        echo -e "${YELLOW}   - DHAN_CLIENT_ID${NC}"
        echo -e "${YELLOW}   - DHAN_ACCESS_TOKEN${NC}"
        echo ""
        read -p "Press Enter after you've edited the .env file..." dummy
    else
        echo -e "${RED}ERROR: .env.example not found!${NC}"
        exit 1
    fi
else
    echo -e "${GREEN}✓${NC} .env file already exists"
fi

# Validate .env file
source .env
MISSING_VARS=()

[ -z "$TELEGRAM_API_ID" ] && MISSING_VARS+=("TELEGRAM_API_ID")
[ -z "$TELEGRAM_API_HASH" ] && MISSING_VARS+=("TELEGRAM_API_HASH")
[ -z "$TARGET_CHANNEL" ] && MISSING_VARS+=("TARGET_CHANNEL")
[ -z "$DHAN_CLIENT_ID" ] && MISSING_VARS+=("DHAN_CLIENT_ID")
[ -z "$DHAN_ACCESS_TOKEN" ] && MISSING_VARS+=("DHAN_ACCESS_TOKEN")

if [ ${#MISSING_VARS[@]} -gt 0 ]; then
    echo -e "${RED}ERROR: Missing required environment variables:${NC}"
    printf '%s\n' "${MISSING_VARS[@]}"
    exit 1
fi

echo -e "${GREEN}✓${NC} Environment configuration validated"

# ==============================================
# 7. Setup Systemd Service (Optional)
# ==============================================
echo ""
echo -e "${YELLOW}[7/9]${NC} Setting up systemd service..."

read -p "Do you want to install as a systemd service (auto-start on boot)? (y/N) " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]; then
    # Get current user
    CURRENT_USER=$(whoami)

    # Create service file
    SERVICE_FILE="/etc/systemd/system/trading-bot.service"

    echo "Creating service file: $SERVICE_FILE"

    sudo tee "$SERVICE_FILE" > /dev/null <<EOF
[Unit]
Description=Trading Bot - Telegram Signal Processor
After=network.target network-online.target
Wants=network-online.target
StartLimitIntervalSec=0

[Service]
Type=simple
User=$CURRENT_USER
Group=$CURRENT_USER
WorkingDirectory=$SCRIPT_DIR
Environment="PATH=$SCRIPT_DIR/.venv/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"

ExecStart=$SCRIPT_DIR/.venv/bin/python main.py

Restart=always
RestartSec=10
StartLimitBurst=5

MemoryMax=1G
CPUQuota=50%

NoNewPrivileges=true
PrivateTmp=true
ProtectSystem=strict
ProtectHome=read-only
ReadWritePaths=$SCRIPT_DIR/logs
ReadWritePaths=$SCRIPT_DIR/cache
ReadWritePaths=$SCRIPT_DIR
ProtectKernelTunables=true
ProtectKernelModules=true
ProtectControlGroups=true

StandardOutput=journal
StandardError=journal
SyslogIdentifier=trading-bot

[Install]
WantedBy=multi-user.target
EOF

    # Reload systemd and enable service
    sudo systemctl daemon-reload
    sudo systemctl enable trading-bot.service

    echo -e "${GREEN}✓${NC} Systemd service installed"
    echo ""
    echo -e "${BLUE}Service commands:${NC}"
    echo -e "  Start:   ${GREEN}sudo systemctl start trading-bot${NC}"
    echo -e "  Stop:    ${GREEN}sudo systemctl stop trading-bot${NC}"
    echo -e "  Status:  ${GREEN}sudo systemctl status trading-bot${NC}"
    echo -e "  Logs:    ${GREEN}sudo journalctl -u trading-bot -f${NC}"
else
    echo -e "${YELLOW}Skipping systemd service setup${NC}"
fi

# ==============================================
# 8. Test Run (Optional)
# ==============================================
echo ""
echo -e "${YELLOW}[8/9]${NC} Testing installation..."

read -p "Do you want to run a quick test? (y/N) " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]; then
    echo "Running verification test..."
    .venv/bin/python verify_deployment.py || {
        echo -e "${YELLOW}WARNING: Test failed. Please check the errors above.${NC}"
    }
fi

# ==============================================
# 9. Finalize
# ==============================================
echo ""
echo -e "${YELLOW}[9/9]${NC} Finalizing deployment..."

# Make scripts executable
chmod +x start_bot.sh
chmod +x deploy.sh

# Create health check script
cat > health_check.sh <<'HEALTHEOF'
#!/bin/bash
# Simple health check script

if screen -list | grep -q "trading_bot"; then
    echo "✓ Bot is running"
    exit 0
else
    echo "✗ Bot is NOT running"
    exit 1
fi
HEALTHEOF

chmod +x health_check.sh

echo -e "${GREEN}✓${NC} Deployment complete!"

# ==============================================
# Final Summary
# ==============================================
echo ""
echo -e "${BLUE}================================================${NC}"
echo -e "${GREEN}  Deployment Successful!${NC}"
echo -e "${BLUE}================================================${NC}"
echo ""
echo -e "${BLUE}📁 Project Directory:${NC} $SCRIPT_DIR"
echo -e "${BLUE}🐍 Python Version:${NC} $(python3 --version)"
echo -e "${BLUE}📦 Virtual Environment:${NC} $SCRIPT_DIR/.venv"
echo ""
echo -e "${BLUE}🚀 To start the bot:${NC}"
echo -e "   ${GREEN}./start_bot.sh start${NC}"
echo ""
echo -e "${BLUE}🛑 To stop the bot:${NC}"
echo -e "   ${GREEN}./start_bot.sh stop${NC}"
echo ""
echo -e "${BLUE}📊 To view logs:${NC}"
echo -e "   ${GREEN}tail -f logs/trade_logs.log${NC}"
echo ""
echo -e "${BLUE}❌ To view errors only:${NC}"
echo -e "   ${GREEN}tail -f logs/errors.log${NC}"
echo ""
echo -e "${BLUE}💚 Health Check:${NC}"
echo -e "   ${GREEN}./health_check.sh${NC}"
echo ""
echo -e "${YELLOW}⚠  Important Notes:${NC}"
echo "  • Make sure your .env file has correct credentials"
echo "  • First run will download ~500MB Dhan master CSV"
echo "  • Logs are in: logs/ directory"
echo "  • Signal data saved to: signals.jsonl and signals.json"
echo ""
echo -e "${GREEN}Happy Trading! 🎯${NC}"
echo ""
