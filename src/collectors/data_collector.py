import os
import json
from tweepy import Client
from dotenv import load_dotenv


class DataCollector:
    def __init__(self):
        # Load environment variables
        load_dotenv()
        self.twitter_bearer_token = os.getenv("TWITTER_BEARER_TOKEN")
        if not self.twitter_bearer_token:
            raise ValueError("Twitter Bearer Token is not set in .env")

        # Initialize Tweepy Client
        self.client = Client(bearer_token=self.twitter_bearer_token)

        # Set the raw data path relative to the project root
        self.raw_data_path = os.path.abspath(
            os.path.join(os.path.dirname(__file__), "../../data/raw")
        )

    def fetch_posts_by_username(self, username, max_results=10):
        """
        Fetch recent tweets from a specific user by username with date information.

        Args:
            username (str): The username of the target user (e.g., "news_account").
            max_results (int): Maximum number of tweets to fetch (up to 100).

        Returns:
            list: A list of dictionaries containing tweet details (text and date).
        """
        try:
            # Fetch user details to get the user ID
            user = self.client.get_user(username=username, user_fields=["id"])
            user_id = user.data.id

            # Fetch recent tweets by user ID
            response = self.client.get_users_tweets(
                id=user_id,
                tweet_fields=["created_at"],
                max_results=max_results,
                exclude = ["replies"]
            )

            # Extract relevant tweet data (text and date)
            tweets = [
                {
                    "text": tweet.text,
                    "date": str(tweet.created_at.date())  # Extract only the date part
                }
                for tweet in response.data
            ]
            return tweets
        except Exception as e:
            print(f"Error fetching tweets by username '{username}': {e}")
            return []

    def save_to_json(self, data, filename="data.json"):
        """
        Save fetched posts to a JSON file in the raw data directory.
        """
        os.makedirs(self.raw_data_path, exist_ok=True)
        file_path = os.path.join(self.raw_data_path, filename)
        try:
            with open(file_path, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=4)
            print(f"Data successfully saved to {file_path}")
        except Exception as e:
            print(f"Error saving data to JSON: {e}")
