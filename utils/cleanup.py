#!/usr/bin/env python3
"""
Cleanup Script - Clears signal files and logs.

Run manually or via systemd timer for automatic cleanup.
Keeps active_trades.json and allowed_phones.json intact.

Usage:
    python cleanup.py          # Dry run (shows what would be deleted)
    python cleanup.py --run    # Actually delete files
"""

import argparse
import logging
from datetime import datetime
from pathlib import Path

BASE_DIR = Path('/opt/trading_bot')
DATA_DIR = BASE_DIR / 'data'
LOGS_DIR = BASE_DIR / 'logs'

# Files to delete
PATTERNS_TO_DELETE = [
    (DATA_DIR, 'signals.json*'),  # signals.json, signals.jsonl
    (LOGS_DIR, '*.log*'),  # trade.log, trade.log.1, etc.
]

# Files to KEEP (never delete)
PROTECTED_FILES = {'active_trades.json', 'allowed_phones.json'}

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
)
logger = logging.getLogger('Cleanup')


def get_files_to_delete() -> list[Path]:
    """Find all files matching cleanup patterns."""
    files = []
    for directory, pattern in PATTERNS_TO_DELETE:
        if directory.exists():
            for f in directory.glob(pattern):
                if f.is_file() and f.name not in PROTECTED_FILES:
                    files.append(f)
    return sorted(files)


def format_size(size_bytes: int) -> str:
    """Format bytes as human-readable string."""
    for unit in ['B', 'KB', 'MB', 'GB']:
        if size_bytes < 1024:
            return f'{size_bytes:.1f}{unit}'
        size_bytes /= 1024
    return f'{size_bytes:.1f}TB'


def main():
    parser = argparse.ArgumentParser(description='Cleanup signal files and logs')
    parser.add_argument('--run', action='store_true', help='Actually delete files')
    args = parser.parse_args()

    files = get_files_to_delete()

    if not files:
        logger.info('Nothing to clean up!')
        return

    total_size = 0
    logger.info('Files to delete:')
    for f in files:
        size = f.stat().st_size
        total_size += size
        logger.info(f'  {f} ({format_size(size)})')

    logger.info(f'Total: {len(files)} files, {format_size(total_size)}')

    if not args.run:
        logger.info('Dry run - use --run to actually delete files')
        return

    # Delete files
    deleted = 0
    for f in files:
        try:
            f.unlink()
            deleted += 1
        except Exception as e:
            logger.error(f'Failed to delete {f}: {e}')

    logger.info(f'Deleted {deleted}/{len(files)} files')
    logger.info(f'Cleanup completed at {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')


if __name__ == '__main__':
    main()
