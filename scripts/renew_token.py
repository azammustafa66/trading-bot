#!/usr/bin/env python3
"""
Dhan Token Renewal Script
-------------------------
Logs in to Dhan using Client ID, PIN, and TOTP Secret.
Updates the DHAN_ACCESS_TOKEN in the .env file.
"""

import logging
import os
import re
import sys
import time
from logging.handlers import RotatingFileHandler

import pyotp
from dhanhq import DhanLogin
from dotenv import load_dotenv

# Path to the .env file and log file
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
ENV_PATH = os.path.join(BASE_DIR, '.env')
LOG_FILE = os.path.join(BASE_DIR, 'logs', 'trade.log')


# Setup Logging
def setup_logging():
    logger = logging.getLogger('TokenRenewal')
    logger.setLevel(logging.INFO)

    formatter = logging.Formatter(
        '%(asctime)s [%(levelname)s] [%(name)s] %(message)s', datefmt='%Y-%m-%d %H:%M:%S'
    )

    # File Handler
    os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True)
    file_handler = RotatingFileHandler(
        LOG_FILE, maxBytes=10 * 1024 * 1024, backupCount=5)
    file_handler.setFormatter(formatter)

    # Console Handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)

    if not logger.handlers:
        logger.addHandler(file_handler)
        logger.addHandler(console_handler)

    return logger


logger = setup_logging()


def renew_token():
    # Load current env vars
    load_dotenv(ENV_PATH)

    client_id = os.getenv('DHAN_CLIENT_ID')
    pin = os.getenv('DHAN_PIN')
    totp_secret = os.getenv('DHAN_TOTP_SECRET', '').strip()

    if not all([client_id, pin, totp_secret]):
        logger.error(
            'Missing credentials in .env (DHAN_CLIENT_ID, DHAN_PIN, or DHAN_TOTP_SECRET)')
        sys.exit(1)

    logger.info(f'Logging in with Client ID: {client_id}')

    max_retries = 3
    for attempt in range(1, max_retries + 1):
        try:
            # Generate TOTP
            totp = pyotp.TOTP(totp_secret).now()

            # Login
            dhan = DhanLogin(client_id)
            response = dhan.generate_token(pin, totp)

            # Handle both camelCase (API) and snake_case (potential wrapper) keys
            new_token = response.get(
                'accessToken') or response.get('access_token')

            if new_token:
                logger.info(
                    f'Login Successful on attempt {attempt}. New Token Generated.')
                # Update .env file
                update_env_file(new_token)
                logger.info('Successfully updated .env file.')
                return  # Success, exit function

            logger.warning(
                f'Login Failed on attempt {attempt}! Response: {response}')

        except Exception as e:
            logger.error(f'Error during renewal on attempt {attempt}: {e}')

        if attempt < max_retries:
            logger.info('Retrying in 2 seconds...')
            time.sleep(2)

    logger.error(f'All {max_retries} attempts failed. Exiting.')
    sys.exit(1)


def update_env_file(new_token):
    with open(ENV_PATH, 'r') as f:
        content = f.read()

    # Regex to find and replace the token line
    # Handles DHAN_ACCESS_TOKEN=current_token or DHAN_ACCESS_TOKEN="current_token"
    pattern = r'(DHAN_ACCESS_TOKEN\s*=\s*)(["\']?)(.+?)(["\']?)(\s|$)'

    if not re.search(pattern, content):
        # Append if not found
        if not content.endswith('\n'):
            content += '\n'
        content += f'DHAN_ACCESS_TOKEN={new_token}\n'
    else:
        # Replace existing
        content = re.sub(pattern, f'\\1\\2{new_token}\\4\\5', content)

    with open(ENV_PATH, 'w') as f:
        f.write(content)


if __name__ == '__main__':
    renew_token()
