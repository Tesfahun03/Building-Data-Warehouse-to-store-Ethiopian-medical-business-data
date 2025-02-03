import pandas as pd
import logging
import re
import os
import emoji

# Ensure logs folder exists
os.makedirs("../logs", exist_ok=True)

# Configure logging to write to file & display in Jupyter Notebook
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("../logs/data_cleaning.log"),  # Log to file
        logging.StreamHandler()  # Log to Jupyter Notebook output
    ]
)


def load_csv(file_path):
    """ Load CSV file into a Pandas DataFrame. """
    try:
        df = pd.read_csv(file_path)
        logging.info(f" CSV file '{file_path}' loaded successfully.")
        return df
    except Exception as e:
        logging.error(f" Error loading CSV file: {e}")
        raise


def remove_emojis(text):
    """ Remove emojis from the message text. """
    return ''.join(c for c in text if c not in emoji.EMOJI_DATA)


def remove_youtube_links(text):
    """ Remove YouTube links from the message text. """
    youtube_pattern = r"https?://(?:www\.)?(?:youtube\.com|youtu\.be)/[^\s]+"
    return re.sub(youtube_pattern, '', text).strip()


def clean_text(text):
    """ Standardize text by removing newline characters and unnecessary spaces. """
    if pd.isna(text):
        return "No Message"
    return re.sub(r'\n+', ' ', text).strip()


def clean_dataframe(df):
    """ Perform all cleaning and standardization steps while avoiding SettingWithCopyWarning. """
    try:

        #  Convert Date to datetime format, replacing NaT with None
        df.loc[:, 'date'] = pd.to_datetime(df['date'], errors='coerce')
        df.loc[:, 'date'] = df['date'].where(df['date'].notna(), None)
        logging.info(" Date column formatted to datetime.")

        #  Convert 'ID' to integer for PostgreSQL BIGINT compatibility
        df.loc[:, 'message_id'] = pd.to_numeric(
            df['message_id'], errors="coerce").fillna(0).astype(int)

        #  Fill missing values
        df.loc[:, 'message_text'] = df['message_text'].fillna("No Message")
        df.loc[:, 'media_path'] = df['media_path'].fillna("No Media")
        logging.info(" Missing values filled.")

        #  Standardize text columns
        df.loc[:, 'channel_title'] = df['channel_title'].str.strip()
        df.loc[:, 'channel_username'] = df['channel_username'].str.strip()
        df.loc[:, 'message_text'] = df['message_text'].apply(clean_text)
        df.loc[:, 'media_path'] = df['media_path'].str.strip()
        logging.info(" Text columns standardized.")

        #  Remove emojis from message text
        df.loc[:, 'message_text'] = df['message_text'].apply(remove_emojis)

        #  Remove YouTube links from message text
        df.loc[:, 'message_text'] = df['message_text'].apply(
            remove_youtube_links)

        #  Rename columns to match PostgreSQL schema
        df = df.rename(columns={
            "channel_title": "channel_title",
            "channel_username": "channel_username",
            "message_id": "message_id",
            "message_text": "message",
            "date": "message_date",
            "media_path": "media_path"
        })

        logging.info(" Data cleaning completed successfully.")
        return df
    except Exception as e:
        logging.error(f" Data cleaning error: {e}")
        raise


def save_cleaned_data(df, output_path):
    """ Save cleaned data to a new CSV file. """
    try:
        df.to_csv(output_path, index=False)
        logging.info(f" Cleaned data saved successfully to '{output_path}'.")
        print(f" Cleaned data saved successfully to '{output_path}'.")
    except Exception as e:
        logging.error(f" Error saving cleaned data: {e}")
        raise
