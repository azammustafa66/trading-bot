import os

from dotenv import load_dotenv
from telethon.sync import TelegramClient

# Load environment variables
load_dotenv(os.path.join(os.path.dirname(os.path.dirname(__file__)), '.env'))

api_id = os.getenv('TELEGRAM_API_ID', '')
api_hash = os.getenv('TELEGRAM_API_HASH', '')
session_name = os.getenv('SESSION_NAME', 'telegram_session')

print('Connecting to Telegram...')
with TelegramClient(session_name, int(api_id), api_hash) as client:
    print('\n--- Your Channels ---')
    # Iterate over all dialogs (chats/channels/groups)
    for dialog in client.iter_dialogs():
        if dialog.is_channel:
            print(f'Name: {dialog.name}')
            print(f'ID:   {dialog.id}')
            print('-' * 20)
    print(
        '\nCopy the ID of the channel you want to listen to and paste it '
        'into TARGET_CHANNEL_ID in .env'
    )
