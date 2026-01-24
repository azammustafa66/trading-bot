import os

from dotenv import load_dotenv

load_dotenv()

client_id = os.getenv('DHAN_CLIENT_ID', '')
token = os.getenv('DHAN_ACCESS_TOKEN', '')

print(f'Client ID present: {bool(client_id)}')
print(f'Token present: {bool(token)}')
if token:
    print(f'Token length: {len(token)}')
    print(f'Token start: {token[:5]}...')
