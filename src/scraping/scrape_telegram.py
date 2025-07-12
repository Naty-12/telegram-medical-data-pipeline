import os
import json
import asyncio
from datetime import datetime
from telethon import TelegramClient
from dotenv import load_dotenv
import logging

# Set up logging
today_str = datetime.now().strftime("%Y-%m-%d")
log_dir = "logs"
os.makedirs(log_dir, exist_ok=True)
log_file_path = os.path.join(log_dir, f"scrape_{today_str}.log")

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file_path),
        logging.StreamHandler()
    ]
)

# Load environment variables
load_dotenv()
API_ID = int(os.getenv('TELEGRAM_API_ID'))
API_HASH = os.getenv('TELEGRAM_API_HASH')
SESSION_NAME = 'telegram_scraper_session'

# Channels to scrape
TELEGRAM_CHANNELS = [
    'CheMed123',
    'lobelia4cosmetics',
    'tikvahpharma',
    # add more here
]

# Data lake paths
DATA_LAKE_BASE_PATH = 'data/raw/telegram_messages'
IMAGES_BASE_PATH = 'data/raw/telegram_images'

async def scrape_channel(client, channel_entity, limit=None):
    channel_name = channel_entity.username if channel_entity.username else str(channel_entity.id)
    logging.info(f"Scraping channel: {channel_name} (ID: {channel_entity.id})")

    all_messages_data = []
    messages_downloaded = 0

    try:
        async for message in client.iter_messages(channel_entity, limit=limit):
            message_date_str = message.date.strftime('%Y-%m-%d')
            channel_data_path = os.path.join(DATA_LAKE_BASE_PATH, message_date_str, channel_name)
            os.makedirs(channel_data_path, exist_ok=True)

            # Build a lightweight JSON structure
            message_data = {
                "id": message.id,
                "date": message.date.isoformat(),
                "text": message.text,
                "sender_id": message.sender_id,
                "media_type": None,
                "file_path": None
            }

            # Handle photo downloads
            if message.photo:
                image_date_path = os.path.join(IMAGES_BASE_PATH, message_date_str, channel_name)
                os.makedirs(image_date_path, exist_ok=True)
                image_filename = f"message_{message.id}_{message.photo.id}.jpg"
                image_filepath = os.path.join(image_date_path, image_filename)
                logging.info(f"Downloading image from message {message.id} to {image_filepath}")
                try:
                    await client.download_media(message.photo, file=image_filepath)
                    message_data["media_type"] = "photo"
                    message_data["file_path"] = image_filepath
                except Exception as e:
                    logging.error(f"Error downloading image from message {message.id}: {e}")

            # Write individual JSON file
            message_filepath = os.path.join(channel_data_path, f'{message.id}.json')
            with open(message_filepath, 'w', encoding='utf-8') as f:
                json.dump(message_data, f, ensure_ascii=False, indent=4)

            all_messages_data.append(message_data)
            messages_downloaded += 1

            if messages_downloaded % 100 == 0:
                logging.info(f"Downloaded {messages_downloaded} messages from {channel_name}")

    except Exception as e:
        logging.error(f"Error scraping channel {channel_name}: {e}")

    # Write metadata summary file
    metadata = {
        "channel": channel_name,
        "scrape_date": datetime.now().isoformat(),
        "messages_downloaded": messages_downloaded
    }
    metadata_path = os.path.join(DATA_LAKE_BASE_PATH, today_str, channel_name, "_scrape_metadata.json")
    os.makedirs(os.path.dirname(metadata_path), exist_ok=True)
    with open(metadata_path, 'w', encoding='utf-8') as f:
        json.dump(metadata, f, ensure_ascii=False, indent=4)

    logging.info(f"Finished scraping {messages_downloaded} messages from {channel_name}")
    return all_messages_data

async def main():
    client = TelegramClient(SESSION_NAME, API_ID, API_HASH)

    logging.info("Starting Telegram client...")
    await client.start()
    logging.info("Telegram client started.")

    # Ensure data directories exist
    os.makedirs(DATA_LAKE_BASE_PATH, exist_ok=True)
    os.makedirs(IMAGES_BASE_PATH, exist_ok=True)

    for channel_username in TELEGRAM_CHANNELS:
        try:
            entity = await client.get_entity(channel_username)
            await scrape_channel(client, entity, limit=None)
        except Exception as e:
            logging.error(f"Could not get entity or scrape for channel {channel_username}: {e}")
            # Optional: handle flood waits or retries here

    logging.info("All scraping tasks completed.")
    await client.run_until_disconnected()
    logging.info("Telegram client disconnected.")

if __name__ == '__main__':
    asyncio.run(main())