#!/bin/bash

# ==============================================
# Update Cron Job Paths
# ==============================================
# For users who already have cron jobs with old paths
# This updates them to use the new scripts/ directory
# ==============================================

set -e

# Colors
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
RED='\033[0;31m'
NC='\033[0m'

echo -e "${BLUE}================================================${NC}"
echo -e "${BLUE}  Updating Cron Job Paths${NC}"
echo -e "${BLUE}================================================${NC}"
echo ""

# Get project directory
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

echo -e "${YELLOW}Project directory: ${GREEN}$PROJECT_DIR${NC}"
echo ""

# Check if user has existing cron jobs
if ! crontab -l 2>/dev/null | grep -q "trading"; then
    echo -e "${YELLOW}No existing trading bot cron jobs found.${NC}"
    echo -e "${BLUE}Run this instead:${NC}"
    echo -e "  ${GREEN}./scripts/setup_schedule.sh${NC}"
    exit 0
fi

# Backup existing crontab
BACKUP_FILE="/tmp/crontab_backup_$(date +%Y%m%d_%H%M%S).txt"
echo "Creating backup..."
crontab -l > "$BACKUP_FILE"
echo -e "${GREEN}✓${NC} Backup saved to: $BACKUP_FILE"
echo ""

# Show what will be updated
echo -e "${YELLOW}Old paths that will be updated:${NC}"
crontab -l | grep trading || true
echo ""

# Confirm with user
read -p "Update cron job paths? (y/N) " -n 1 -r
echo
if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    echo "Cancelled."
    exit 0
fi

# Update the paths
echo "Updating paths..."

# Get current crontab
TEMP_CRON=$(mktemp)
crontab -l > "$TEMP_CRON"

# Update paths using sed
sed -i "s|/pre_download_csv.sh|/scripts/pre_download_csv.sh|g" "$TEMP_CRON"
sed -i "s|/monitor.py|/scripts/monitor.py|g" "$TEMP_CRON"

# Install updated crontab
crontab "$TEMP_CRON"
rm "$TEMP_CRON"

echo -e "${GREEN}✓${NC} Paths updated successfully!"
echo ""

# Show updated cron jobs
echo -e "${BLUE}Updated cron jobs:${NC}"
crontab -l | grep trading || true
echo ""

echo -e "${GREEN}✅ Done!${NC}"
echo ""
echo -e "${BLUE}Verify with:${NC}"
echo -e "  ${GREEN}crontab -l${NC}"
echo ""
