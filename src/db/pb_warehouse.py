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

    def get_dataset(self) -> list[Record]:
        """Fetch the dataset from the PocketBase warehouse."""
        try:
            dataset = self.client.collection("dataset").get_full_list(batch=10000)
            return dataset
        except ClientResponseError as e:
            logger.error(f"Error fetching dataset: {e}")
            return []

    def ingest_single_tweet(self, tweet: Tweet) -> Record:
        """Ingest a single Tweet instance into the PocketBase warehouse."""
        try:
            assert isinstance(tweet, Tweet), "Input must be a Tweet instance"
            assert tweet.user_id, "Tweet must have a user_id"

            updatedRecord = self.client.collection("tweets_v2").create(
                tweet.model_dump()
            )

            logger.success(
                f"Successfully created tweet record with ID: {updatedRecord.id}"
            )
            return updatedRecord
        except ClientResponseError as e:
            if "validation_not_unique" in str(e):
                logger.info(f"Tweet with ID {tweet.tweet_id} already exists. Skipping.")
                return None
            else:
                logger.error(f"Error ingesting tweet: {e}")
                return None
        except Exception as e:
            logger.error(f"Error ingesting tweet: {e}, {tweet}")
            return None

    def ingest_tweet(self, tweet: dict) -> dict[str, Record]:
        assert isinstance(tweet, dict), "Input must be a dictionary"
        processed_tweet = self._process_tweet(tweet)
        processed_user = self._process_user(tweet)

        record_tweet: Record = None
        record_user: Record = None

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
            else:
                logger.error(f"Error ingesting user: {e}")
        except Exception as e:
            logger.error(f"Error ingesting user: {e}, {processed_user}")
        return {
            "record_tweet": record_tweet,
            "record_user": record_user,
        }

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

    def get_user_by_id(self, user_id: str) -> Record:
        assert isinstance(user_id, str), "user_id must be a string"
        user = self.client.collection("tweet_users").get_first_list_item(
            f"user_id = '{user_id}'"
        )
        return user

    def get_tweet_with_no_classification(self) -> Record:
        """Get a tweet that has not been classified yet."""
        tweetRecord = self.client.collection("tweets_v2").get_first_list_item(
            "is_hateful = NULL",
        )
        return tweetRecord

    def get_user_with_not_fetched_tweets(
        self, less_than_k_tweets: int | None = None
    ) -> Record:
        """Get a user that has 'not fetched' tweets yet."""

        filter_condition = "status = 'not fetched' || status = NULL"
        if less_than_k_tweets is not None:
            filter_condition += f" && number_of_tweets < {less_than_k_tweets}"

        userRecord = self.client.collection("users_tweets_status").get_first_list_item(
            filter_condition,
        )
        userRecord = self.get_user_by_id(userRecord.user_id)

        return userRecord
