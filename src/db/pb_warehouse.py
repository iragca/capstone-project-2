from pocketbase import PocketBase
from pocketbase.errors import ClientResponseError
from pocketbase.models import Record

from ..config import Settings as s, logger
from ..models import Tweet, User


class PBWarehouse:
    def __init__(self, url: str = s.POCKETBASE_URL.value):
        self.client = PocketBase(url)
        self.authenticated = self.client.admins.auth_with_password(
            email=s.POCKETBASE_EMAIL.value, password=s.POCKETBASE_PASSWORD.value
        )

    def ingest_tweet(self, tweet: dict) -> dict[str, Record]:
        assert isinstance(tweet, dict), "Input must be a dictionary"
        processed_tweet = self._process_tweet(tweet)
        processed_user = self._process_user(tweet)

        try:
            record_tweet = self.client.collection("tweets_v2").create(processed_tweet)
            logger.success(
                f"Successfully created tweet record with ID: {record_tweet.id}"
            )
        except ClientResponseError as e:
            if "validation_not_unique" in str(e):
                logger.info(
                    f"Tweet with ID {processed_tweet['tweet_id']} already exists. Skipping."
                )

        try:
            record_user = self.client.collection("tweet_users").create(processed_user)
            logger.success(
                f"Successfully created user record with ID: {record_user.id}"
            )
        except ClientResponseError as e:
            if "validation_not_unique" in str(e):
                logger.info(
                    f"User with ID {processed_user['user_id']} already exists. Skipping."
                )
        return {
            "record_tweet": record_tweet if record_tweet else None,
            "record_user": record_user if record_user else None,
        }

    def update_tweet_community_note(self, tweet: dict) -> Record:
        assert isinstance(tweet, dict), "Input must be a dictionary"
        processed_tweet: dict = self._process_tweet(tweet)
        if processed_tweet["community_note"] is None:
            logger.info("No community note provided. Skipping update.")
            return None

        tweet_id: str = processed_tweet.get("tweet_id")

        record: Record = self.client.collection("tweets_v2").get_first_list_item(
            f"tweet_id = '{tweet_id}'"
        )
        if not record:
            raise ClientResponseError(
                f"Tweet with ID {tweet_id} not found.", status=404
            )

        record_tweet = Tweet(**record.__dict__)
        if record_tweet.community_note:
            logger.info(
                f"Tweet with ID {tweet_id} already has a community note. Skipping update."
            )
            return record

        updated_record = self.client.collection("tweets_v2").update(
            record.id, {"community_note": processed_tweet["community_note"]}
        )
        logger.success(
            f"Successfully updated community note for tweet with ID: {tweet_id}"
        )
        return updated_record

    @staticmethod
    def _process_tweet(tweet: dict) -> dict[str, any]:
        assert isinstance(tweet, dict), "Input must be a dictionary"

        text = tweet.get("text", "")
        user_id = tweet.get("user", {}).get("user_id", "")
        username = tweet.get("user", {}).get("username", "")
        status_link = f"https://x.com/{username}/status/{tweet.get('tweet_id', '')}"
        retweet_status_id = tweet.get("retweet_tweet_id", {})
        quoted_status_id = tweet.get("quoted_status_id", {})
        community_note = tweet.get("community_note", {})
        search_terms = [
            "#blacklivesmatter",
            "#blm",
            "#blacklivesmatters",
        ]

        tweet["has_blm_hashtag"] = any(term in text.lower() for term in search_terms)
        tweet["user_id"] = user_id
        tweet["status_link"] = status_link
        tweet["retweet_status_id"] = retweet_status_id
        tweet["quoted_status_id"] = quoted_status_id
        tweet["community_note"] = (
            community_note.get("subtitle", {}).get("text", "")
            if community_note
            else None
        )

        parsed_tweet = Tweet(**tweet)
        return parsed_tweet.model_dump()

    @staticmethod
    def _process_user(tweet: dict) -> dict[str, any]:
        assert isinstance(tweet, dict), "Input must be a dictionary"
        user = tweet.get("user", {})
        parsed_user = User(**user)
        return parsed_user.model_dump()

    def update_has_fetched_replies(self, tweet_id: str) -> Record:
        assert isinstance(tweet_id, str), "tweet_id must be a string"
        record = self.client.collection("tweets_v2").get_list(
            1, 1, {"filter": f"tweet_id = '{tweet_id}'"}
        )

        updated_record = self.client.collection("tweets_v2").update(
            record.items[0].id, {"fetched_replies": True}
        )
        return updated_record

    def get_user_by_id(self, user_id: str) -> dict[str, any]:
        assert isinstance(user_id, str), "user_id must be a string"
        user = self.client.collection("tweet_users").get_list(
            1, 1, {"filter": f"user_id = '{user_id}'"}
        )

        if user.items and len(user.items) > 0:
            return user.items[0].__dict__
        return {}
