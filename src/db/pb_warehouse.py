from pocketbase import PocketBase

from src.config import Settings as s
from src.models import Tweet, User


class PBWarehouse:
    def __init__(self):
        self.client = PocketBase(s.POCKETBASE_URL.value)
        self.authenticated = self.client.admins.auth_with_password(
            email=s.POCKETBASE_EMAIL.value, password=s.POCKETBASE_PASSWORD.value
        )

    def ingest_tweet(self, tweet: dict):
        assert isinstance(tweet, dict), "Input must be a dictionary"
        processed_tweet = self._process_tweet(tweet)
        processed_user = self._process_user(tweet)
        record_tweet = self.client.collection("tweets_v2").create(processed_tweet)
        record_user = self.client.collection("tweet_users").create(processed_user)
        return {
            "record_tweet": record_tweet,
            "record_user": record_user,
        }

    @staticmethod
    def _process_tweet(tweet: dict):
        assert isinstance(tweet, dict), "Input must be a dictionary"

        text = tweet.get("text", "")
        user_id = tweet.get("user", {}).get("user_id", "")
        username = tweet.get("user", {}).get("username", "")
        status_link = f"https://x.com/{username}/status/{tweet.get('tweet_id', '')}"

        search_terms = [
            "#blacklivesmatter",
            "#blm",
            "#blacklivesmatters",
        ]

        tweet["has_blm_hashtag"] = any(term in text.lower() for term in search_terms)
        tweet["user_id"] = user_id
        tweet["status_link"] = status_link

        parsed_tweet = Tweet(**tweet)
        return parsed_tweet.model_dump()

    @staticmethod
    def _process_user(tweet: dict):
        assert isinstance(tweet, dict), "Input must be a dictionary"
        user = tweet.get("user", {})
        parsed_user = User(**user)
        return parsed_user.model_dump()

    def update_has_fetched_replies(self, tweet_id: str) -> None:
        assert isinstance(tweet_id, str), "tweet_id must be a string"
        record = self.client.collection("tweets_v2").get_list(1, 1, {"filter": f"tweet_id = '{tweet_id}'"})
        
        updated_record = self.client.collection("tweets_v2").update(
            record.items[0].id,
            {"fetched_replies": True}
        )
        return updated_record
