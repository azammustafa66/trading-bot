import os
from datetime import date, datetime

import requests
from dotenv import load_dotenv

# Load credentials
load_dotenv()
CLIENT_ID = os.getenv('DHAN_CLIENT_ID')
ACCESS_TOKEN = os.getenv('DHAN_ACCESS_TOKEN')
BASE_URL = 'https://api.dhan.co/v2'


def get_public_ip():
    """Gets the outbound IP of this droplet."""
    try:
        return requests.get('https://api.ipify.org?format=json', timeout=5).json()['ip']
    except Exception as e:
        print(f'❌ Could not determine Public IP: {e}')
        return None


def get_dhan_ip_status():
    """Fetches currently whitelisted IPs from Dhan."""
    url = f'{BASE_URL}/ip/getIP'
    headers = {'access-token': ACCESS_TOKEN, 'Accept': 'application/json'}
    try:
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            return response.json()
        else:
            print(f'⚠️ API Error (Get IP): {response.text}')
            return None
    except Exception as e:
        print(f'❌ Connection Failed: {e}')
        return None


def set_or_modify_ip(target_ip, current_config):
    """Decides whether to SET or MODIFY the IP."""

    headers = {
        'access-token': ACCESS_TOKEN,
        'Content-Type': 'application/json',
        'Accept': 'application/json',
    }

    payload = {'dhanClientId': CLIENT_ID, 'ip': target_ip, 'ipFlag': 'PRIMARY'}

    # 1. Check if IP is already correct
    current_primary = current_config.get('primaryIP', '')
    if current_primary == target_ip:
        print(f'✅ SUCCESS: Your IP {target_ip} is already whitelisted!')
        return

    # 2. Determine Endpoint (SET vs MODIFY)
    # If primaryIP is empty or null, we use SET. If it has a value, we use MODIFY.
    if not current_primary:
        print('🚀 No IP found. Setting PRIMARY IP...')
        endpoint = '/ip/setIP'
        method = requests.post
    else:
        print(f'⚠️ Different IP found ({current_primary}). Attempting MODIFY...')

        # Check 7-day lock
        mod_date_str = current_config.get('modifyDatePrimary')
        if mod_date_str:
            mod_date = datetime.strptime(mod_date_str, '%Y-%m-%d').date()
            if date.today() < mod_date:
                print(f'❌ BLOCKED: You cannot modify IP until {mod_date_str} (7-day lock).')
                print('👉 Use the Web Portal if this is urgent.')
                return

        endpoint = '/ip/modifyIP'
        method = requests.put

    # 3. Execute Request
    try:
        response = method(f'{BASE_URL}{endpoint}', headers=headers, json=payload)
        data = response.json()

        if response.status_code == 200 and data.get('status') == 'SUCCESS':
            print(f'🎉 SUCCESS! IP Whitelisted: {target_ip}')
        else:
            print(f'❌ FAILED: {data}')

    except Exception as e:
        print(f'❌ Request Error: {e}')


if __name__ == '__main__':
    if not CLIENT_ID or not ACCESS_TOKEN:
        print('❌ Error: DHAN_CLIENT_ID or DHAN_ACCESS_TOKEN missing in .env')
        exit(1)

    print('🔍 detecting droplet IP...')
    my_ip = get_public_ip()

    if my_ip:
        print(f'📍 Detected Public IP: {my_ip}')

        print('📡 Checking Dhan Status...')
        config = get_dhan_ip_status()

        if config is not None:
            set_or_modify_ip(my_ip, config)
    else:
        print('❌ Aborted. Could not detect IP.')
