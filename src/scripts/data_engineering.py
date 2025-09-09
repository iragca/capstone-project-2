import json
import time

from pocketbase.errors import ClientResponseError
from pocketbase.models import Record
from tqdm import tqdm
from typer import Typer

from src.config import (
    INTERIM_DATA_DIR,
    LOGGER_DIR,
    PROJECT_ROOT,
    logger,
)
from src.db import PBWarehouse
from src.models import Tweet, User
from src.utils import ensure_path, function_logger
from src.utils.parsers import api45_tweet, api45_user

cli = Typer()


@cli.command()
@function_logger(LOGGER_DIR=LOGGER_DIR)
def update_user_friends_count() -> None:
    """
    Update the `friends_count` field for users in the PocketBase warehouse.

    This function iterates over locally saved tweet JSON files in the
    `INTERIM_DATA_DIR/twitter_api45` staging area, extracts user
    information, and compares each user's friends count with the stored
    value in PocketBase. If there is a mismatch, the PocketBase record is
    updated accordingly.

    Parameters
    ----------
    None

    Returns
    -------
    None
        The function performs side effects:
        - Reads tweet JSON files from the staging directory.
        - Extracts user information using `api45_user`.
        - Updates the corresponding `tweet_users` record in PocketBase
        when the `friends_count` differs.
        - Logs progress, warnings, and errors.

    Raises
    ------
    AssertionError
        If the loaded tweet file does not contain a dictionary.
    json.JSONDecodeError
        If a tweet file cannot be parsed as JSON.
    Exception
        Catches and logs any other unexpected errors during processing.

    Notes
    -----
    - Non-JSON files in the staging directory are skipped automatically.
    - Users not found in PocketBase are logged and skipped.
    - If a user's `friends_count` already matches the value in PocketBase,
      no update is performed.

    See Also
    --------
    get_all_users_tweets_by_oldbird : Fetches user-specific tweets.
    get_info_of_users : Collects metadata for users.

    Examples
    --------
    Run as a CLI command:

    >>> uv run scraping update-user-friends-count
    """
    SAVE_DIR = ensure_path(INTERIM_DATA_DIR / "twitter_api45")
    pb = PBWarehouse()

    for tweet_file in SAVE_DIR.iterdir():
        if tweet_file.suffix != ".json":
            logger.warning(f"Skipping non-JSON file: {tweet_file.name}")
            continue

        try:
            with open(tweet_file, "r", encoding="utf-8") as f:
                tweet_data: dict = json.load(f)
                assert isinstance(tweet_data, dict), (
                    f"Tweet data must be a dictionary, got {type(tweet_data)}"
                )

                user_data = tweet_data.get("user_info", {})
                user: User = api45_user(user_data)

                userRecord: Record = pb.get_user_by_id(user.user_id)
                if not userRecord:
                    logger.warning(
                        f"User with ID {user.user_id} not found in PocketBase. Skipping."
                    )
                    continue

                if userRecord.friends == user.friends:
                    logger.info(
                        f"User {user.username} (ID: {user.user_id}) already has friends_count set to {user.friends}. Skipping."
                    )
                    continue

                # Update the user's friends_count
                pb.client.collection("tweet_users").update(
                    userRecord.id,
                    {
                        "friends": user.friends,
                    },
                )
                logger.success(
                    f"Updated friends_count for user {user.username} (ID: {user.user_id}) to {user.friends}."
                )
        except Exception as e:
            logger.error(
                f"Error processing tweet file {tweet_file.name}: {type(e).__name__} - {e}"
            )
            continue


@cli.command()
@function_logger(LOGGER_DIR=LOGGER_DIR, level="WARNING")
def update_user_friends():
    """
    Update the `friends` count for users in the PocketBase warehouse.

    This function iterates over locally saved user JSON files in the
    `INTERIM_DATA_DIR/twitter_api45_user` staging directory, extracts the
    `rest_id` and `friends` values, and updates the corresponding
    `tweet_users` records in PocketBase. Existing users with a
    nonzero `friends` count are skipped.

    Parameters
    ----------
    None

    Returns
    -------
    None
        The function performs side effects:
        - Reads user JSON files from the staging directory.
        - Extracts user IDs (`rest_id`) and friend counts.
        - Updates `tweet_users` records in PocketBase when applicable.
        - Logs progress, warnings, and errors.

    Raises
    ------
    AssertionError
        If the loaded user file does not contain a dictionary.
    json.JSONDecodeError
        If a user file cannot be parsed as JSON.
    ClientResponseError
        If a PocketBase API call fails (e.g., user not found).
    Exception
        Logs and skips any other unexpected errors during processing.

    Notes
    -----
    - Non-JSON files in the staging directory are skipped.
    - Users without a `rest_id` in their JSON file are logged and skipped.
    - If a user already has a `friends` count greater than zero in PocketBase,
      no update is performed.
    - Updates only the `friends` field in the `tweet_users` collection.

    See Also
    --------
    update_user_friends_count : Similar function, but processes tweet files to update friends counts.
    get_info_of_users : Fetches and saves metadata about users.

    Examples
    --------
    Run as a CLI command:

    >>> uv run scraping update-user-friends
    """
    pb = PBWarehouse()
    SAVE_DIR = ensure_path(INTERIM_DATA_DIR / "twitter_api45_user")

    for user_file in SAVE_DIR.iterdir():
        if user_file.suffix != ".json":
            logger.warning(f"Skipping non-JSON file: {user_file.name}")
            continue

        try:
            with open(user_file, "r", encoding="utf-8") as f:
                user_data: dict = json.load(f)
                assert isinstance(user_data, dict), (
                    f"User data must be a dictionary, got {type(user_data)}"
                )

                user_id = user_data.get("rest_id")
                if not user_id:
                    logger.warning(f"User ID not found in {user_file.name}. Skipping.")
                    continue

                try:
                    userRecord: Record = pb.get_user_by_id(str(user_id))

                    if userRecord.friends > 0:
                        logger.info(
                            f"User {userRecord.username} (ID: {userRecord.id}) has {userRecord.friends} friends."
                        )
                        continue
                except ClientResponseError as e:
                    if "The requested resource wasn't found." in str(e):
                        logger.warning(
                            f"User with ID {user_id} not found in PocketBase. Skipping."
                        )

                friend_count = user_data.get("friends", 0)
                pb.client.collection("tweet_users").update(
                    userRecord.id, {"friends": friend_count}
                )
                logger.success(f"Updated friends count for user {user_id}.")

        except Exception as e:
            logger.error(
                f"Error processing user file {user_file.name}: {userRecord.id} {type(e).__name__} - {e}"
            )
            continue


@cli.command()
@function_logger(LOGGER_DIR=LOGGER_DIR, level="WARNING")
def ingest_data_from_api45():
    """
    Ingest data from the Twitter API45 into the PocketBase warehouse.

    This function iterates over JSON files in the `INTERIM_DATA_DIR/twitter_api45` directory,
    parses each tweet, and ingests it into the PocketBase collection. If a tweet contains a
    quoted tweet, the quoted tweet is ingested as well.

    Files that are not valid JSON or missing required fields are skipped with a warning.

    Returns
    -------
    None
        The function performs side effects:
        - Reads tweets from JSON files in `INTERIM_DATA_DIR/twitter_api45`.
        - Ingests tweets into PocketBase using `PBWarehouse.ingest_single_tweet`.
        - Logs progress, warnings, and errors.

    Raises
    ------
    AssertionError
        If the loaded JSON data is not a dictionary.
    Exception
        If there is an error processing a tweet or quoted tweet.

    Examples
    --------
    Run as a CLI command:

    >>> uv run scraping ingest-data-from-api45
    """
    SAVE_PATH = ensure_path(INTERIM_DATA_DIR / "twitter_api45")
    pb = PBWarehouse()

    for tweet_file in SAVE_PATH.iterdir():
        if tweet_file.suffix != ".json":
            logger.warning(f"Skipping non-JSON file: {tweet_file.name}")
            continue

        try:
            with open(tweet_file, "r", encoding="utf-8") as f:
                tweet_data: dict = json.load(f)
                assert isinstance(tweet_data, dict), (
                    f"Tweet data must be a dictionary, got {type(tweet_data)}"
                )

                if tweet_data["tweet_id"] is None:
                    logger.warning(
                        f"Tweet data does not contain 'tweet_id'. Skipping {tweet_file.name}."
                    )
                    continue

                tweet: Tweet = api45_tweet(tweet_data)
                pb.ingest_single_tweet(tweet)
                # If the tweet has a retweet or quoted status, ingest those as well

                try:
                    if "quoted" in tweet_data.keys():
                        if tweet_data["quoted"]["tweet_id"] is not None:
                            quote = api45_tweet(tweet_data["quoted"])
                            pb.ingest_single_tweet(quote)
                except Exception as e:
                    logger.error(
                        f"Error processing quoted tweet in file {tweet_file.name}: {type(e).__name__} - {e}"
                    )

        except Exception as e:
            logger.error(
                f"Error processing tweet file {tweet_file.name}: {type(e).__name__} - {e}"
            )
            continue


@cli.command()
@function_logger(LOGGER_DIR=LOGGER_DIR)
def ingest_tweety_tweets() -> None:
    """
    Ingest tweets from Tweety into the PocketBase warehouse.

    This function reads JSON files from the `INTERIM_DATA_DIR/tweety` staging area, validates
    each tweet, and ingests them into the `tweets_v2` collection in PocketBase. Files that are
    not valid JSON or contain invalid data are skipped, and existing tweets are not duplicated.

    Returns
    -------
    None
        The function performs side effects:
        - Reads tweets from JSON files in `INTERIM_DATA_DIR/tweety`.
        - Validates the structure of each tweet.
        - Ingests valid tweets into the PocketBase `tweets_v2` collection.
        - Logs progress, warnings, and errors.

    Raises
    ------
    AssertionError
        If the JSON data is not a list or if any tweet is not a dictionary or missing the
        required `tweet_id`.
    ClientResponseError
        If there is an error from PocketBase when ingesting a tweet.
    Exception
        For any other errors during processing.

    Examples
    --------
    Run as a CLI command:

    >>> uv run scraping ingest-tweety-tweets
    """
    pb = PBWarehouse()
    staging_area = INTERIM_DATA_DIR / "tweety"

    for user_tweets_file in staging_area.iterdir():
        if user_tweets_file.suffix != ".json":
            logger.warning(f"Skipping non-JSON file: {user_tweets_file.name}")
            continue

        try:
            with open(user_tweets_file, "r", encoding="utf-8") as f:
                tweets = json.load(f)

            assert isinstance(tweets, list), (
                f"Tweets data must be a list, got {type(tweets)}"
            )

            for tweet in tweets:
                assert isinstance(tweet, dict), (
                    f"Each tweet must be a dictionary, got {type(tweet)}"
                )
                assert "tweet_id" in tweet, "Tweet data must contain 'tweet_id'"
                pb.client.collection("tweets_v2").create(tweet)

            logger.success(f"Successfully ingested tweets from {user_tweets_file.name}")

        except ClientResponseError as e:
            if "validation_not_unique" in str(e):
                logger.info(
                    f"Tweet with ID {tweet['tweet_id']} already exists. Skipping."
                )
                continue
            else:
                logger.error(
                    f"ClientResponseError while ingesting {user_tweets_file.name}: {e}"
                )
        except Exception as e:
            logger.error(
                f"Error ingesting {user_tweets_file.name}: {type(e).__name__} - {e}"
            )


@cli.command()
def update_has_replies_using_reply_id() -> None:
    """
    Update the `has_replies` (fetched_replies) field for tweets in the PocketBase warehouse.

    This function iterates over all tweets that are replies (`in_reply_to_status_id` not NULL),
    and marks the corresponding parent tweet's `fetched_replies` field as True if the reply exists
    in the PocketBase `tweets_v2` collection.

    Returns
    -------
    None
        The function performs side effects:
        - Queries tweets with non-null `in_reply_to_status_id`.
        - Updates the parent tweet in PocketBase to indicate that it has replies.
        - Logs warnings for missing reply records and errors during processing.

    Raises
    ------
    Exception
        For unexpected errors when querying or updating PocketBase records.

    Examples
    --------
    Run as a CLI command:

    >>> uv run scraping update-has-replies-using-reply-id
    """
    logger.add(PROJECT_ROOT / "reports" / "logs" / "update_has_replies.logs")

    pb = PBWarehouse()
    records: list[Record] = pb.client.collection("tweets_v2").get_full_list(
        query_params={"filter": "in_reply_to_status_id != NULL"}
    )

    for record in tqdm(
        records, desc="Updating has_replies", unit="tweet", leave=False, ncols=100
    ):
        tweet = Tweet(**record.__dict__)
        try:
            if tweet.in_reply_to_status_id:
                reply_record = pb.client.collection("tweets_v2").get_list(
                    1, 1, {"filter": f"tweet_id = '{tweet.in_reply_to_status_id}'"}
                )
                if not reply_record.items:
                    logger.warning(
                        f"No reply record found for tweet_id {tweet.in_reply_to_status_id}. Skipping."
                    )
                    continue

                pb.client.collection("tweets_v2").update(
                    reply_record.items[0].id, {"fetched_replies": True}
                )

        except Exception as e:
            logger.error(f"Error updating tweet {record.id}: {type(e).__name__} - {e}")
            continue


@cli.command()
@function_logger(LOGGER_DIR=LOGGER_DIR, level="WARNING")
def clean_fetching_buffer() -> None:
    """
    Reset the `status` of users marked as 'fetching' in the PocketBase warehouse.

    This function is intended to clear the "fetching buffer" for users in the
    `tweet_users` collection. It updates any users currently marked as `status='fetching'`
    back to `status='not fetched'`. A countdown is displayed before starting to allow
    cancellation if fetching is actually in progress.

    Returns
    -------
    None
        Performs side effects:
        - Iterates over users with `status='fetching'`.
        - Updates each user's status to `not fetched`.
        - Logs progress and any errors encountered.

    Warnings
    --------
    Ensure that no active fetching processes are running before executing this command,
    as this may interfere with ongoing data collection.

    Raises
    ------
    Exception
        For unexpected errors during querying or updating PocketBase records.

    Examples
    --------
    Run as a CLI command:

    >>> uv run scraping clean-fetching-buffer
    """
    pb = PBWarehouse()

    print("Make sure no one is actually fetching tweets before running this command.")
    for second in range(10):
        time.sleep(1)
        print(f"Cleaning fetching buffer in {10 - second} seconds. Ctrl+C to cancel.")
    have_data = True
    while have_data:
        try:
            userRecord: Record = pb.client.collection(
                "tweet_users"
            ).get_first_list_item("status = 'fetching'")
            user = User(**userRecord.__dict__)
            pb.client.collection("tweet_users").update(
                userRecord.id, {"status": "not fetched"}
            )
            logger.info(
                f"Cleaned fetching buffer for user: {user.username} (ID: {user.user_id})"
            )
        except ClientResponseError as e:
            if "The requested resource wasn't found." in str(e):
                logger.info("No more users with fetching status to clean.")
                have_data = False
            else:
                logger.error(f"ClientResponseError: {e}")
                have_data = False
        except Exception as e:
            logger.error(f"Error cleaning fetching buffer: {type(e).__name__} - {e}")
            have_data = False


@cli.command()
@function_logger(LOGGER_DIR=LOGGER_DIR, level="WARNING")
def reset_user_fetched_tweets() -> None:
    """
    Reset the `status` of all users who have `fetched_tweets` in the PocketBase warehouse.

    This function prompts the user for confirmation before iterating through the
    `tweet_users` collection. Any user with `status='fetched'` will be updated to
    `status='not fetched'`. A countdown is displayed before performing the reset
    to allow cancellation.

    Returns
    -------
    None
        Performs side effects:
        - Prompts the user for confirmation.
        - Iterates over users with `status='fetched'`.
        - Updates each user's status to `not fetched`.
        - Logs progress, warnings, and errors.

    Warnings
    --------
    This operation affects all users marked as fetched. Ensure no active fetching
    processes are running when executing this command.

    Raises
    ------
    Exception
        For unexpected errors during querying or updating PocketBase records.

    Examples
    --------
    Run as a CLI command:

    >>> uv run scraping reset-user-fetched-tweets
    """
    pb = PBWarehouse()

    user_input: str = ""

    while user_input not in ["yes", "no"]:
        user_input = (
            input(
                "This will reset the fetched_tweets field for all users. "
                "Are you sure you want to continue? (yes/no): "
            )
            .strip()
            .lower()
        )

        if user_input not in ["yes", "no"]:
            logger.warning("Please enter 'yes' or 'no'.")
            continue

    if user_input == "no":
        logger.info("Operation cancelled by user.")
        return

    for second in range(10):
        time.sleep(1)
        logger.info(
            f"Resetting fetched_tweets for all users in {10 - second} seconds. Ctrl+C to cancel."
        )

    have_data = True
    while have_data:
        try:
            userRecord: Record = pb.client.collection(
                "tweet_users"
            ).get_first_list_item("status = 'fetched'")
            user = User(**userRecord.__dict__)
            pb.client.collection("tweet_users").update(
                userRecord.id, {"status": "not fetched"}
            )
            logger.info(
                f"Reset fetched_tweets for user: {user.username} (ID: {user.user_id})"
            )
        except ClientResponseError as e:
            if "The requested resource wasn't found." in str(e):
                logger.info("No more users with fetched_tweets to reset.")
                have_data = False
            else:
                logger.error(f"ClientResponseError: {e}")
                have_data = False
        except Exception as e:
            logger.error(f"Error resetting fetched_tweets: {type(e).__name__} - {e}")
            have_data = False


@cli.command()
def update_has_replies_using_has_blm() -> None:
    """
    Update the `has_replies` field for tweets that are replies to BLM-related tweets.

    This function fetches all tweets from the `tweets_v2` collection where
    `is_reply_to_blm` is `TRUE`. For each such tweet, if it references another tweet
    via `in_reply_to_status_id`, the corresponding tweet record is updated to set
    `fetched_replies=True`.

    The function logs warnings when a reply record is not found and errors for any
    unexpected exceptions.

    Returns
    -------
    None
        Performs side effects:
        - Queries the PocketBase `tweets_v2` collection.
        - Updates `fetched_replies` to `True` for relevant tweets.
        - Logs progress, warnings, and errors.

    Raises
    ------
    Exception
        For unexpected errors while querying or updating PocketBase records.

    Examples
    --------
    Run as a CLI command:

    >>> uv run scraping update-has-replies-using-has-blm
    """
    logger.add(PROJECT_ROOT / "reports" / "logs" / "update_has_replies.logs")

    pb = PBWarehouse()
    records: list[Record] = pb.client.collection("tweets_v2").get_full_list(
        query_params={"filter": "is_reply_to_blm = TRUE"}
    )

    for record in tqdm(
        records, desc="Updating has_replies", unit="tweet", leave=False, ncols=100
    ):
        tweet = Tweet(**record.__dict__)
        try:
            if tweet.in_reply_to_status_id:
                reply_record = pb.client.collection("tweets_v2").get_list(
                    1, 1, {"filter": f"tweet_id = '{tweet.in_reply_to_status_id}'"}
                )

                if not reply_record.items:
                    logger.warning(
                        f"No reply record found for tweet_id {tweet.in_reply_to_status_id}. Skipping."
                    )
                    continue

                pb.client.collection("tweets_v2").update(
                    reply_record.items[0].id, {"fetched_replies": True}
                )

        except Exception as e:
            logger.error(f"Error updating tweet {record.id}: {type(e).__name__} - {e}")
            continue


@cli.command()
def update_reply_links() -> None:
    """
    Update the `in_reply_to_status_link` field for tweets.

    This function iterates over all tweets in the `tweets_v2` collection where
    `in_reply_to_status_link` is `NULL` and `in_reply_to_status_id` is not `NULL`.
    For each tweet, it constructs a URL linking to the original tweet based on the
    replied user's username and the tweet ID, then updates the `in_reply_to_status_link`
    field in PocketBase.

    Warnings are logged if:
    - The tweet already has a `in_reply_to_status_link`.
    - The original tweet record cannot be found.
    - The original user record cannot be found.
    Errors are logged for any unexpected exceptions during processing.

    Returns
    -------
    None
        Performs side effects:
        - Queries `tweets_v2` and `tweet_users` collections.
        - Updates the `in_reply_to_status_link` field for relevant tweets.
        - Logs progress, warnings, and errors.

    Raises
    ------
    Exception
        For unexpected errors while querying or updating PocketBase records.

    Examples
    --------
    Run as a CLI command:

    >>> uv run scraping update-reply-links
    """

    logger.add(PROJECT_ROOT / "reports" / "logs" / "update_reply_links.logs")

    pb = PBWarehouse()
    records: list[Record] = pb.client.collection("tweets_v2").get_full_list(
        query_params={
            "filter": "in_reply_to_status_link = NULL && in_reply_to_status_id != NULL"
        }
    )

    for record in tqdm(
        records, desc="Updating reply links", unit="tweet", leave=False, ncols=100
    ):
        try:
            if record.in_reply_to_status_link:
                logger.warning(
                    f"Tweet {record.id} already has in_reply_to_status_link. Skipping."
                )
                continue

            if record.in_reply_to_status_id:
                reply_record = pb.client.collection("tweets_v2").get_list(
                    1, 1, {"filter": f"tweet_id = '{record.in_reply_to_status_id}'"}
                )

                if not reply_record.items:
                    logger.warning(
                        f"No reply record found for tweet_id {record.in_reply_to_status_id}. Skipping."
                    )
                    continue

                replied_user = pb.client.collection("tweet_users").get_list(
                    1, 1, {"filter": f"user_id = '{reply_record.items[0].user_id}'"}
                )

                if not replied_user.items:
                    logger.warning(
                        f"No user record found for user_id {reply_record.items[0].user_id}. Skipping."
                    )
                    continue

                assert record.id, "Record ID is required to update the tweet."
                assert replied_user.items, "Replied user must exist to create a link."
                assert reply_record.items, "Reply record must exist to create a link."
                assert isinstance(record.id, str), "Record ID must be a string."
                reply_link = f"https://x.com/{replied_user.items[0].username}/status/{reply_record.items[0].tweet_id}"
                pb.client.collection("tweets_v2").update(
                    record.id, {"in_reply_to_status_link": reply_link}
                )
            else:
                continue
        except Exception as e:
            logger.error(f"Error updating tweet {record.id}: {type(e).__name__} - {e}")
            continue

    logger.success("All tweets updated successfully.")


@cli.command()
def update_is_reply_to_blm() -> None:
    """
    Update the `is_reply_to_blm` field for tweets.

    This function iterates over all tweets in the `tweets_v2` collection. For each tweet
    that is a reply (`in_reply_to_status_id` is not `None`), it checks whether the original
    tweet has the `has_blm_hashtag` field set to `True`. If so, it updates the current tweet's
    `is_reply_to_blm` field to `True`.

    Warnings are logged if:
    - The original tweet (being replied to) cannot be found.
    Errors are logged for any unexpected exceptions during processing.

    Returns
    -------
    None
        Performs side effects:
        - Queries the `tweets_v2` collection.
        - Updates the `is_reply_to_blm` field for relevant tweets.
        - Logs progress, warnings, and errors.

    Raises
    ------
    Exception
        For unexpected errors while querying or updating PocketBase records.

    Examples
    --------
    Run as a CLI command:

    >>> uv run scraping update-is-reply-to-blm
    """
    logger.add(PROJECT_ROOT / "reports" / "logs" / "update_is_reply.logs")

    pb = PBWarehouse()
    records: list[Record] = pb.client.collection("tweets_v2").get_full_list()

    for record in tqdm(records, desc="Updating tweets", unit="tweet"):
        try:
            reply_to = record.in_reply_to_status_id
            if not reply_to:
                continue
            if reply_to:
                reply_record = pb.client.collection("tweets_v2").get_list(
                    1, 1, {"filter": f"tweet_id = '{reply_to}'"}
                )

                if not reply_record.items:
                    logger.warning(
                        f"No reply record found for tweet_id {reply_to}. Skipping."
                    )
                    continue
                if reply_record.items[0].has_blm_hashtag:
                    pb.client.collection("tweets_v2").update(
                        record.id, {"is_reply_to_blm": True}
                    )
        except Exception as e:
            logger.error(f"Error updating tweet {record.id}: {type(e).__name__} - {e}")
            continue

    logger.success("All tweets updated successfully.")


@cli.command()
@function_logger(LOGGER_DIR=LOGGER_DIR, level="WARNING")
def ingest_data() -> None:
    """
    Ingest tweets from the staging area into the PocketBase warehouse.

    This function iterates over JSON files in the staging area (`INTERIM_DATA_DIR/oldbird`).
    For each tweet file, it:

    1. Loads the tweet data as a dictionary.
    2. Validates that the dictionary contains a `tweet_id`.
    3. Ingests retweeted or quoted tweets if present.
    4. Ingests the main tweet into the PocketBase warehouse.

    Warnings are logged if a file is not a JSON file. Errors during ingestion are logged
    with details.

    Returns
    -------
    None
        Performs side effects:
        - Reads JSON tweet files from the staging area.
        - Ingests tweets (including retweets and quoted tweets) into the PocketBase warehouse.
        - Logs progress, warnings, and errors.

    Raises
    ------
    AssertionError
        If a tweet file does not contain a dictionary or `tweet_id`.
    Exception
        For unexpected errors during file reading or ingestion.

    Examples
    --------
    Run as a CLI command:

    >>> uv run scraping ingest-data
    """
    pb_client = PBWarehouse()
    staging_area = INTERIM_DATA_DIR / "oldbird"
    logger.info(f"Staging area: {staging_area}")

    for tweet_file in staging_area.iterdir():
        if tweet_file.suffix == ".json":
            try:
                with open(tweet_file, "r", encoding="utf-8") as f:
                    tweet_data = json.load(f)

                assert isinstance(tweet_data, dict), "Tweet data must be a dictionary"
                assert "tweet_id" in tweet_data, "Tweet data must contain 'tweet_id'"

                if tweet_data["retweet_status"]:
                    pb_client.ingest_tweet(tweet_data["retweet_status"])
                if tweet_data["quoted_status"]:
                    pb_client.ingest_tweet(tweet_data["quoted_status"])

                pb_client.ingest_tweet(tweet_data)
            except Exception as e:
                logger.error(
                    f"Error ingesting {tweet_file.name}: {type(e).__name__} - {e}"
                )
        else:
            logger.warning(f"Skipping non-JSON file: {tweet_file.name}")


if __name__ == "__main__":
    cli()
