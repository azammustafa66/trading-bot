#!/bin/bash

# ==============================================
# Pre-download Dhan Master CSV
# ==============================================
# Downloads the CSV before market opens
# Runs at 8:50 AM via cron
# ==============================================

# Get the directory where this script is located
PROJECT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
VENV_PYTHON="$PROJECT_DIR/.venv/bin/python"

echo "[$(date)] ========================================"
echo "[$(date)] Pre-downloading Dhan Master CSV"
echo "[$(date)] ========================================"

# Check if virtual environment exists
if [ ! -f "$VENV_PYTHON" ]; then
    echo "[$(date)] ERROR: Virtual environment not found at $VENV_PYTHON"
    exit 1
fi

# Run the download script
cd "$PROJECT_DIR"
$VENV_PYTHON -c "
from core.dhan_mapper import DhanMapper
import logging

logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

print('[INFO] Initializing Dhan Mapper...')
mapper = DhanMapper()

# This will download CSV if it doesn't exist or is old
mapper._ensure_csv()

print('[INFO] CSV check complete!')
"

EXIT_CODE=$?

if [ $EXIT_CODE -eq 0 ]; then
    echo "[$(date)] ✅ CSV download/check completed successfully"
else
    echo "[$(date)] ❌ CSV download/check failed with exit code $EXIT_CODE"
fi

echo "[$(date)] ========================================"
exit $EXIT_CODE
