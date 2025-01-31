from dotenv import load_dotenv
from telethon import TelegramClient

import os
import sys
import asyncio
import json
import logging
import pandas as pd
import asyncio

sys.path.append(os.path.abspath('..'))

logging.basicConfig(
    filename='../logs/tg_scrapper.log',
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

#channel_usernames = ['@yetenaweg', '@lobelia4cosmetics','@CheMed123', '@DoctorsET']
channel_usernames = ['@DoctorsET']


load_dotenv()
api_id = os.getenv('api_id')
api_hash = os.getenv('api_hash')


async def fetch_messages(client, channel_username):
    logging.info(f"Fetching messages from channel: {channel_username}")
    messages_data = []
    channel = await client.get_entity(channel_username)
    logging.info(
        f"Channel details - Title: {channel.title}, Username: {channel.username}, ID: {channel.id}")

    os.makedirs("photos", exist_ok=True)  # Ensure the photos folder exists

    async for message in client.iter_messages(channel_username, limit=10):
        media_path = None
        if message.media:
            media_path = await client.download_media(message, file=f"../data/photos/{channel_username[1:]}-{message.id}.jpg")
            logging.info(f"Downloaded media to {media_path}")

        messages_data.append({
            "channel_title": channel.title,
            "channel_username": channel_username,
            "channel_id": channel.id,
            "message_text": message.text,
            "date": message.date,
            "media_path": media_path
        })

    logging.info(f"Fetched {len(messages_data)} messages.")
    return messages_data


async def main():

    logging.info('client connected successfully ')

    # Fetch message from the channels above
    async with TelegramClient('session_name', api_id, api_hash) as client:
        tasks = [fetch_messages(client, username)
                 for username in channel_usernames]
        results = await asyncio.gather(*tasks)

        # Flatten the list of messages and process as needed
        all_messages = [msg for sublist in results for msg in sublist]
    os.makedirs("../data", exist_ok=True)
    # save message in csv format
    output_file = '../data/telegram_messages.csv'

    if all_messages:  # Only save if there are messages
        df = pd.DataFrame(all_messages, columns=["channel_title", "channel_username", "channel_id",
                                                 "message_text", "date", "media_path"])
        df.to_csv(output_file, index=False)
        logging.info(f"Messages saved to {output_file}")
    else:
        logging.info("No messages to save.")

    logging.info('client disconnected successfully ')


if __name__ == "__main__":
    asyncio.run(main())
