from typing import Union

from pocketbase import PocketBase
from pocketbase.errors import ClientResponseError
from pocketbase.models import Record

from ..config import Settings as s
from ..config import logger
from ..models import Tweet, User
from ..utils import check_type


class PBWarehouse:
    """
    Service wrapper for PocketBase.

    Handles authentication and provides methods for ingesting and retrieving
    Tweet and User records.

    Args:
        url (str, optional): Base URL for PocketBase. Defaults to config value.

    Raises:
        ConnectionError: If the PocketBase server cannot be reached.

    Methods:
        get_dataset() -> list[Record]:
            Fetch all records from the "dataset" collection.

        ingest_user(user: User) -> Record | None:
            Ingest a User into the "tweet_users" collection.

        ingest_single_tweet(tweet: Tweet) -> Record | None:
            Ingest a Tweet into the "tweets_v2" collection.

        ingest_tweet(tweet: dict) -> dict[str, Record]:
            Ingest both tweet and user data from a dict.

        update_has_fetched_replies(tweet_id: str) -> Record:
            Mark a tweet as having fetched replies.

        get_user_by_id(user_id: str) -> Record:
            Fetch user by ID.

        get_user_by_username(username: str, strict: bool) -> Record:
            Fetch user by username.

        get_tweet_with_no_classification(collection: str) -> Record:
            Fetch a tweet with no classification.

        get_user_with_not_fetched_tweets(less_than_k_tweets: int | None) -> Record:
            Fetch a user with "not fetched" tweets.

        does_user_exist(user_id: str | None, username: str | None, strict: bool) -> bool:
            Check if a user exists.

        get_tweet_by_id(tweet_id: str | int) -> Record:
            Fetch tweet by ID.
    """

    def __init__(self, url: str = s.POCKETBASE_URL.value):
        self.client = PocketBase(url)

        try:
            self.authenticated = self.client.admins.auth_with_password(
                email=s.POCKETBASE_EMAIL.value, password=s.POCKETBASE_PASSWORD.value
            )
        except ClientResponseError as e:
            err_msg = str(e)
            if "No route to host" in err_msg:
                self.authenticated = False
                raise ConnectionError(
                    "Cannot connect to the PocketBase server. Is it running?"
                )
            else:
                self.authenticated = False
                raise e

    def get_dataset(self) -> list[Record]:
        """Fetch the dataset from the PocketBase warehouse."""
        try:
            dataset = self.client.collection("dataset").get_full_list(batch=10000)
            return dataset
        except ClientResponseError as e:
            logger.error(f"Error fetching dataset: {e}")
            return []

    def _ingest_record(
        self, record: Union[User, Tweet], model_name: str, collection_name: str
    ) -> Union[Record, None]:
        """Generic method to ingest a record into a PocketBase collection."""
        try:
            new_record = self.client.collection(collection_name).create(
                record.model_dump()
            )

            logger.success(
                f"Successfully created {model_name} record with ID: {new_record.id}"
            )
            return new_record

        except ClientResponseError as e:
            if "validation_not_unique" in str(e):
                record_id = getattr(record, f"{model_name}_id", "unknown")
                logger.info(
                    f"{model_name.capitalize()} with ID {record_id} already exists. Skipping."
                )
                return None
            else:
                logger.error(f"Error ingesting {model_name}: {e}")
                return None

        except Exception as e:
            logger.error(f"Unexpected error ingesting {model_name}: {e}, {record}")
            return None

    def ingest_user(self, user: User) -> Union[Record, None]:
        """Ingest a single User instance into the PocketBase warehouse."""
        check_type(user, User, "user")
        return self._ingest_record(user, "user", "tweet_users")

    def ingest_single_tweet(self, tweet: Tweet) -> Union[Record, None]:
        """Ingest a single Tweet instance into the PocketBase warehouse."""
        check_type(tweet, Tweet, "tweet")
        if not tweet.user_id:
            logger.error("Tweet must have a user_id.")
            return None
        return self._ingest_record(tweet, "tweet", "tweets_v2")

    def ingest_tweet(self, tweet: dict) -> dict[str, Record]:
        check_type(tweet, dict, "tweet")
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
        check_type(tweet, dict, "tweet")

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
        check_type(tweet, dict, "tweet")
        user = tweet.get("user", {})
        parsed_user = User(**user)
        return parsed_user.model_dump()

    def update_has_fetched_replies(self, tweet_id: str) -> Record:
        check_type(tweet_id, str, "tweet_id")
        record = self.client.collection("tweets_v2").get_list(
            1, 1, {"filter": f"tweet_id = '{tweet_id}'"}
        )

        updated_record = self.client.collection("tweets_v2").update(
            record.items[0].id, {"fetched_replies": True}
        )
        return updated_record

    def get_user_by_id(self, user_id: str) -> Record:
        check_type(user_id, str, "user_id")
        user = self.client.collection("tweet_users").get_first_list_item(
            f"user_id = '{user_id}'"
        )
        return user

    def get_user_by_username(self, username: str, strict: bool = True) -> Record:
        check_type(username, str, "username")
        check_type(strict, bool, "strict")

        if strict:
            filter = f"username = '{username}'"
        else:
            filter = f"username ~ '{username}'"

        user = self.client.collection("tweet_users").get_first_list_item(filter)
        return user

    def get_tweet_with_no_classification(self, collection: str = "tweets_v2") -> Record:
        """Get a tweet that has not been classified yet."""
        tweetRecord = self.client.collection(collection).get_first_list_item(
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

    def does_user_exist(
        self, user_id: str | None, username: str | None, strict: bool = True
    ) -> bool:
        """Check if a user exists in the PocketBase warehouse."""
        if not user_id and not username:
            raise ValueError("Either User ID or Username must be provided.")

        try:
            if user_id:
                check_type(user_id, str, "user_id")
                self.get_user_by_id(user_id)
                return True

            if username:
                check_type(username, str, "username")
                self.get_user_by_username(username, strict=strict)
                return True
        except ClientResponseError as e:
            if "The requested resource wasn't found." in str(e):
                return False
            else:
                return False

    def get_tweet_by_id(self, tweet_id: str | int) -> Record:
        """Get a tweet by its ID."""

        if isinstance(tweet_id, int):
            try:
                tweet_id = str(tweet_id)
            except ValueError:
                raise ValueError("tweet_id must be a string or an integer.")

        if not tweet_id.isdecimal():
            raise ValueError("tweet_id must be a valid decimal string")

        tweet = self.client.collection("tweets_v2").get_first_list_item(
            f"tweet_id = '{tweet_id}'"
        )
        return tweet
