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

    Parameters
    ----------
    url : str, optional
        Base URL for PocketBase. Defaults to the configured value.

    Raises
    ------
    ConnectionError
        If the PocketBase server cannot be reached.
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
        """
        Fetch the dataset from the PocketBase warehouse.

        Returns
        -------
        list of Record
            A list of PocketBase records from the "dataset" collection.
            Returns an empty list if the query fails.

        Raises
        ------
        ClientResponseError
            If PocketBase returns an error response other than handled cases.
        """
        try:
            dataset = self.client.collection("dataset").get_full_list(batch=10000)
            return dataset
        except ClientResponseError as e:
            logger.error(f"Error fetching dataset: {e}")
            return []

    def _ingest_record(
        self, record: Union[User, Tweet], model_name: str, collection_name: str
    ) -> Union[Record, None]:
        """
        Generic method to ingest a record into a PocketBase collection.

        Parameters
        ----------
        record : User or Tweet
            The record object to ingest.
        model_name : str
            The model name ("user" or "tweet").
        collection_name : str
            The target PocketBase collection name.

        Returns
        -------
        Record or None
            The created record, or ``None`` if it already exists or
            ingestion fails.

        Raises
        ------
        ClientResponseError
            If PocketBase returns an unexpected error.
        """
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
        """
        Ingest a single user into the PocketBase warehouse.

        Parameters
        ----------
        user : User
            The user object to ingest.

        Returns
        -------
        Record or None
            The created PocketBase record if successful, or ``None`` if
            the record already exists or an error occurred.

        Raises
        ------
        TypeError
            If ``user`` is not an instance of :class:`~src.models.User`.
        """

        check_type(user, User, "user")
        return self._ingest_record(user, "user", "tweet_users")

    def ingest_single_tweet(self, tweet: Tweet) -> Union[Record, None]:
        """
        Ingest a single tweet into the warehouse.

        Parameters
        ----------
        tweet : Tweet
            The tweet object to ingest.

        Returns
        -------
        Record or None
            The created PocketBase record, or ``None`` if the tweet
            already exists or ingestion fails.

        Raises
        ------
        TypeError
            If ``tweet`` is not an instance of :class:`~src.models.Tweet`.
        ValueError
            If the tweet does not have a ``user_id``.
        """
        check_type(tweet, Tweet, "tweet")
        if not tweet.user_id:
            logger.error("Tweet must have a user_id.")
            return None
        return self._ingest_record(tweet, "tweet", "tweets_v2")

    def ingest_tweet(self, tweet: dict) -> dict[str, Record]:
        """
        Ingest a tweet and its associated user into the PocketBase warehouse.

        Parameters
        ----------
        tweet : dict
            A dictionary representing the tweet, including a nested user
            dictionary under the ``"user"`` key.

        Returns
        -------
        dict of str to Record
            A dictionary containing the created records:
            - ``"record_tweet"`` : Record of the tweet (may be None if skipped)
            - ``"record_user"`` : Record of the user (may be None if skipped)

        Raises
        ------
        TypeError
            If ``tweet`` is not a dict.
        """
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
        """
        Normalize and validate tweet data for ingestion.

        Parameters
        ----------
        tweet : dict
            A dictionary representing the raw tweet.

        Returns
        -------
        dict
            A processed tweet dictionary suitable for PocketBase ingestion.

        Raises
        ------
        TypeError
            If ``tweet`` is not a dict.
        """
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
        """
        Normalize and validate user data from a tweet.

        Parameters
        ----------
        tweet : dict
            A dictionary containing a nested ``"user"`` field.

        Returns
        -------
        dict
            A processed user dictionary suitable for PocketBase ingestion.

        Raises
        ------
        TypeError
            If ``tweet`` is not a dict.
        """
        check_type(tweet, dict, "tweet")
        user = tweet.get("user", {})
        parsed_user = User(**user)
        return parsed_user.model_dump()

    def update_has_fetched_replies(self, tweet_id: str) -> Record:
        """
        Mark a tweet as having fetched replies.

        Parameters
        ----------
        tweet_id : str
            The ID of the tweet to update.

        Returns
        -------
        Record
            The updated tweet record.

        Raises
        ------
        TypeError
            If ``tweet_id`` is not a string.
        """
        check_type(tweet_id, str, "tweet_id")
        record = self.client.collection("tweets_v2").get_list(
            1, 1, {"filter": f"tweet_id = '{tweet_id}'"}
        )

        updated_record = self.client.collection("tweets_v2").update(
            record.items[0].id, {"fetched_replies": True}
        )
        return updated_record

    def get_user_by_id(self, user_id: str) -> Record:
        """
        Fetch a user by ID.

        Parameters
        ----------
        user_id : str
            The ID of the user.

        Returns
        -------
        Record
            The PocketBase user record.

        Raises
        ------
        TypeError
            If ``user_id`` is not a string.
        ClientResponseError
            If the user does not exist or the query fails.
        """
        check_type(user_id, str, "user_id")
        user = self.client.collection("tweet_users").get_first_list_item(
            f"user_id = '{user_id}'"
        )
        return user

    def get_user_by_username(self, username: str, strict: bool = True) -> Record:
        """
        Fetch a user by username.

        Parameters
        ----------
        username : str
            The username to look up.
        strict : bool, default=True
            If True, requires exact match. If False, performs partial match.

        Returns
        -------
        Record
            The PocketBase record for the user.

        Raises
        ------
        ClientResponseError
            If the user does not exist or the query fails.
        """
        check_type(username, str, "username")
        check_type(strict, bool, "strict")

        if strict:
            filter = f"username = '{username}'"
        else:
            filter = f"username ~ '{username}'"

        user = self.client.collection("tweet_users").get_first_list_item(filter)
        return user

    def get_tweet_with_no_classification(self, collection: str = "tweets_v2") -> Record:
        """
        Fetch a tweet that has not been classified.

        Parameters
        ----------
        collection : str, default="tweets_v2"
            The collection to query.

        Returns
        -------
        Record
            The first tweet without classification.
        """
        tweetRecord = self.client.collection(collection).get_first_list_item(
            "is_hateful = NULL",
        )
        return tweetRecord

    def get_user_with_not_fetched_tweets(
        self, less_than_k_tweets: int | None = None
    ) -> Record:
        """
        Fetch a user with tweets not yet fetched.

        Parameters
        ----------
        less_than_k_tweets : int or None, optional
            If provided, restricts results to users with fewer than
            ``k`` tweets.

        Returns
        -------
        Record
            The PocketBase user record.
        """

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
        """
        Check if a user exists in the warehouse.

        Parameters
        ----------
        user_id : str or None
            The user ID, if available.
        username : str or None
            The username, if available.
        strict : bool, default=True
            Whether to require an exact username match.

        Returns
        -------
        bool
            True if the user exists, False otherwise.

        Raises
        ------
        ValueError
            If neither ``user_id`` nor ``username`` are provided.
        """
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
        """
        Fetch a tweet by ID.

        Parameters
        ----------
        tweet_id : str or int
            The tweet ID.

        Returns
        -------
        Record
            The PocketBase tweet record.

        Raises
        ------
        ValueError
            If ``tweet_id`` is invalid or not a decimal string.
        """

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
