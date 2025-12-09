import os

import requests
from dotenv import load_dotenv

# Load credentials
load_dotenv()
CLIENT_ID = os.getenv('DHAN_CLIENT_ID')
ACCESS_TOKEN = os.getenv('DHAN_ACCESS_TOKEN')
BASE_URL = 'https://api.dhan.co/v2'

# --- ⚠️ IMPORTANT: PASTE YOUR RESERVED IP HERE ---
# Do not trust auto-detection since your routing might still be tricky.
TARGET_IP = '129.212.244.81'  # <--- REPLACE THIS WITH YOUR RESERVED IP


def set_secondary_ip(ip):
    """Sets the SECONDARY IP slot since Primary is locked."""
    print(f'🚀 Attempting to whitelist {ip} as SECONDARY IP...')

    url = f'{BASE_URL}/ip/setIP'

    headers = {'access-token': ACCESS_TOKEN, 'Content-Type': 'application/json', 'Accept': 'application/json'}

    payload = {
        'dhanClientId': CLIENT_ID,
        'ip': ip,
        'ipFlag': 'SECONDARY',  # <--- The Magic Fix
    }

    try:
        response = requests.post(url, headers=headers, json=payload)
        data = response.json()

        if response.status_code == 200 and data.get('status') == 'SUCCESS':
            print(f'✅ SUCCESS! Your Droplet IP ({ip}) is now whitelisted (Slot 2).')
            print('👉 You can now trade immediately.')
        else:
            print(f'❌ FAILED: {data}')
            # If setIP fails, sometimes modifyIP works if the slot technically "exists" but is empty
            print('🔄 Trying modifyIP as backup...')
            modify_secondary_ip(ip)

    except Exception as e:
        print(f'❌ Connection Error: {e}')


def modify_secondary_ip(ip):
    url = f'{BASE_URL}/ip/modifyIP'
    payload = {'dhanClientId': CLIENT_ID, 'ip': ip, 'ipFlag': 'SECONDARY'}
    headers = {'access-token': ACCESS_TOKEN, 'Content-Type': 'application/json'}

    resp = requests.put(url, headers=headers, json=payload)
    print(f'Backup Attempt Result: {resp.json()}')


if __name__ == '__main__':
    if TARGET_IP == '49.37.168.175':
        print('❌ ERROR: You forgot to edit the script with your Reserved IP!')
    else:
        set_secondary_ip(TARGET_IP)
