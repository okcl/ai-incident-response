import os
from dotenv import load_dotenv

class ConfigLoader:
    def __init__(self):
        # Load environment variables from .env file
        load_dotenv()

    @property
    def twitter_bearer_token(self):
        return os.getenv("TWITTER_BEARER_TOKEN")

    @property
    def raw_data_path(self):
        return os.getenv("RAW_DATA_PATH", "data/raw")
