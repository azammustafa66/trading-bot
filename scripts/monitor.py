"""
Trading Bot Health Monitor
Monitors the bot's health and can send alerts if issues are detected
"""

import json
import logging
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
)
logger = logging.getLogger('Monitor')


class BotHealthMonitor:
    """Monitors the health of the trading bot"""

    def __init__(self, base_dir=None):
        self.base_dir = Path(base_dir or os.path.dirname(__file__))
        self.logs_dir = self.base_dir / 'logs'
        self.trade_log = self.logs_dir / 'trade_logs.log'
        self.error_log = self.logs_dir / 'errors.log'
        self.signals_file = self.base_dir / 'signals.json'

    def check_process_running(self):
        """Check if the bot process is running"""
        try:
            # Check for screen session
            result = os.popen('screen -list | grep trading_bot').read()
            if 'trading_bot' in result:
                logger.info('✓ Bot process is running (screen session)')
                return True

            # Check for systemd service
            result = os.popen('systemctl is-active trading-bot 2>/dev/null').read().strip()
            if result == 'active':
                logger.info('✓ Bot process is running (systemd service)')
                return True

            logger.error('✗ Bot process is NOT running')
            return False
        except Exception as e:
            logger.error(f'✗ Error checking process: {e}')
            return False

    def check_log_files(self):
        """Check if log files exist and are being written to"""
        issues = []

        # Check main log
        if not self.trade_log.exists():
            issues.append('Main log file does not exist')
        else:
            # Check if log was modified in the last 5 minutes
            mtime = datetime.fromtimestamp(self.trade_log.stat().st_mtime)
            age = datetime.now() - mtime
            if age > timedelta(minutes=5):
                issues.append(f"Main log hasn't been updated in {age.seconds // 60} minutes")
            else:
                logger.info(f'✓ Main log is active (last update: {age.seconds}s ago)')

        # Check error log
        if self.error_log.exists():
            # Count recent errors
            error_count = self._count_recent_errors()
            if error_count > 10:
                issues.append(f'High error count: {error_count} errors in last hour')
            elif error_count > 0:
                logger.warning(f'⚠ {error_count} errors in last hour')
            else:
                logger.info('✓ No recent errors')

        # Check log file sizes
        if self.trade_log.exists():
            size_mb = self.trade_log.stat().st_size / (1024 * 1024)
            if size_mb > 100:
                logger.warning(f'⚠ Main log file is large: {size_mb:.1f} MB')

        if issues:
            for issue in issues:
                logger.error(f'✗ {issue}')
            return False

        return True

    def _count_recent_errors(self, hours=1):
        """Count errors in the last N hours"""
        if not self.error_log.exists():
            return 0

        try:
            cutoff = datetime.now() - timedelta(hours=hours)
            error_count = 0

            with open(self.error_log, 'r') as f:
                for line in f:
                    try:
                        # Parse timestamp from log line
                        timestamp_str = line.split('[')[0].strip()
                        timestamp = datetime.strptime(timestamp_str, '%Y-%m-%d %H:%M:%S')
                        if timestamp > cutoff:
                            error_count += 1
                    except:
                        continue

            return error_count
        except Exception as e:
            logger.warning(f'Could not count errors: {e}')
            return 0

    def check_signal_processing(self):
        """Check if signals are being processed"""
        if not self.signals_file.exists():
            logger.warning('⚠ No signals file found (this is normal if bot just started)')
            return True

        try:
            with open(self.signals_file, 'r') as f:
                signals = json.load(f)

            if not signals:
                logger.info('ℹ No signals processed yet')
                return True

            latest_signal = max(signals, key=lambda x: x.get('timestamp', ''))
            timestamp_str = latest_signal.get('timestamp', '')

            if timestamp_str:
                # Parse timestamp
                try:
                    timestamp = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
                    # Remove timezone for comparison
                    if timestamp.tzinfo:
                        timestamp = timestamp.replace(tzinfo=None)

                    age = datetime.now() - timestamp
                    logger.info(
                        f'✓ Latest signal: {latest_signal.get("trading_symbol")} ({age.seconds // 60}m ago)'
                    )
                except:
                    logger.warning('⚠ Could not parse signal timestamp')

            total_count = len(signals)
            logger.info(f'✓ Total signals processed: {total_count}')

            return True
        except Exception as e:
            logger.error(f'✗ Error checking signals: {e}')
            return False

    def check_disk_space(self):
        """Check available disk space"""
        try:
            stat = os.statvfs(self.base_dir)
            free_gb = (stat.f_bavail * stat.f_frsize) / (1024**3)

            if free_gb < 1:
                logger.error(f'✗ Low disk space: {free_gb:.2f} GB free')
                return False
            elif free_gb < 5:
                logger.warning(f'⚠ Disk space getting low: {free_gb:.2f} GB free')
            else:
                logger.info(f'✓ Disk space: {free_gb:.1f} GB free')

            return True
        except Exception as e:
            logger.warning(f'Could not check disk space: {e}')
            return True

    def check_cache_directory(self):
        """Check if cache directory exists and has the CSV file"""
        cache_dir = self.base_dir / 'cache'

        if not cache_dir.exists():
            logger.warning('⚠ Cache directory does not exist')
            return True

        csv_file = cache_dir / 'dhan_master.csv'
        if not csv_file.exists():
            logger.warning('⚠ Dhan master CSV not downloaded yet')
            return True

        # Check CSV age
        mtime = datetime.fromtimestamp(csv_file.stat().st_mtime)
        age = datetime.now() - mtime

        if age > timedelta(days=1):
            logger.warning(f'⚠ Dhan CSV is {age.days} days old (should refresh daily)')
        else:
            logger.info('✓ Dhan CSV is up to date')

        return True

    def run_full_check(self):
        """Run all health checks"""
        logger.info('=' * 60)
        logger.info('🏥 Trading Bot Health Check')
        logger.info(f'⏰ {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')
        logger.info('=' * 60)

        checks = {
            'Process Running': self.check_process_running(),
            'Log Files': self.check_log_files(),
            'Signal Processing': self.check_signal_processing(),
            'Disk Space': self.check_disk_space(),
            'Cache Directory': self.check_cache_directory(),
        }

        logger.info('=' * 60)
        logger.info('📊 Health Check Summary')
        logger.info('=' * 60)

        all_passed = True
        for check_name, passed in checks.items():
            status = '✓ PASS' if passed else '✗ FAIL'
            logger.info(f'{check_name}: {status}')
            if not passed:
                all_passed = False

        logger.info('=' * 60)

        if all_passed:
            logger.info('✅ All health checks passed!')
            return 0
        else:
            logger.error('❌ Some health checks failed!')
            return 1


def main():
    """Main entry point"""
    monitor = BotHealthMonitor()
    exit_code = monitor.run_full_check()
    sys.exit(exit_code)


if __name__ == '__main__':
    main()
