import asyncio
import calendar
import json
import math
import subprocess

import time
import requests
import torch
from pocketbase.errors import ClientResponseError
from pocketbase.models import Record
from tqdm import tqdm
from typer import Option, Typer

from src.config import (
    INTERIM_DATA_DIR,
    LOGGER_DIR,
    PROJECT_ROOT,
    Settings,
    logger,
)
from src.db import PBWarehouse
from src.models import Tweet, User
from src.scraper import TweetyScraper, RapidApiScraper
from src.utils import ensure_path, function_logger, get_tweet_replies, get_user_tweets
from src.utils.parsers import api45_tweet, api45_user

cli = Typer()


@cli.command()
@function_logger(LOGGER_DIR=LOGGER_DIR, level="WARNING")
def ingest_data_from_api45():
    """Ingest data from the Twitter API45 into the PocketBase warehouse."""
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
def update_user_friends_count() -> None:
    """Update the friends_count field for users in the PocketBase warehouse."""
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
@function_logger(LOGGER_DIR=LOGGER_DIR)
def get_user_tweets_v2(
    max_retries: int = 5,
    less_than_k_tweets: int | None = None,
    max_pages: int | None = None,
) -> None:
    """Get tweets for users using the RapidApi Twitter API45.

    Args:
        max_retries (int): Maximum number of retries for fetching tweets.
                           Useful when the API thinks there are no more tweets
                           for a user, but there are actually more tweets available.
    """
    scraper = RapidApiScraper(api_key=Settings.X_RAPIDAPI_KEY.value)
    pb = PBWarehouse()
    SAVE_DIR = ensure_path(INTERIM_DATA_DIR / "twitter_api45")
    have_data = True
    while have_data:
        try:
            userRecord: Record = pb.get_user_with_not_fetched_tweets(less_than_k_tweets)

            user = User(**userRecord.__dict__)
            logger.info(
                f"Fetching tweets for user: {user.username} (ID: {user.user_id}) - {user.number_of_tweets} tweets"
            )

            pb.client.collection("tweet_users").update(
                userRecord.id, {"status": "fetching"}
            )

            tweets: list[dict] = scraper.get_users_tweets_by_twitter_api45(
                username=user.username,
                expected_num_tweets=user.number_of_tweets,
                max_retries=max_retries,
                max_pages=max_pages,
            )

            if len(tweets) == 0:
                logger.warning(
                    f"No tweets found for user {user.username}. "
                    "Possibly because the tweets are sensitive."
                )
                pb.client.collection("tweet_users").update(
                    userRecord.id, {"status": "fetched"}
                )
                continue

            # Save the fetched tweets to the designated directory
            for tweet in tweets:
                tweet_file = SAVE_DIR / f"{tweet['tweet_id']}.json"
                with open(tweet_file, "w", encoding="utf-8") as f:
                    json.dump(tweet, f)

            pb.client.collection("tweet_users").update(
                userRecord.id,
                {
                    "status": "fetched",
                },
            )
            logger.success(
                f"Fetched tweets from user {user.username} with {len(tweets)} tweets."
            )
        except ClientResponseError as e:
            if "The requested resource wasn't found." in str(e):
                logger.info("No more users to fetch tweets for.")
                pb.client.collection("tweet_users").update(
                    userRecord.id, {"status": "not fetched"}
                )
                have_data = False
            else:
                logger.error(f"ClientResponseError: {e}")
                pb.client.collection("tweet_users").update(
                    userRecord.id, {"status": "not fetched"}
                )
                have_data = False
        except Exception as e:
            logger.error(f"Error fetching tweets: {type(e).__name__} - {e}")
            have_data = False
            pb.client.collection("tweet_users").update(
                userRecord.id, {"status": "not fetched"}
            )


@cli.command()
@function_logger(LOGGER_DIR=LOGGER_DIR)
def ingest_tweety_tweets() -> None:
    """Ingest tweets from Tweety into the PocketBase warehouse."""
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
@function_logger(LOGGER_DIR=LOGGER_DIR)
def get_all_users_tweets_by_oldbird(max_requests: int | None = None) -> None:
    pb = PBWarehouse()
    SAVE_DIR = ensure_path(INTERIM_DATA_DIR / "oldbird")
    TWEETS_PER_PAGE = 20

    filter_params = (
        "creation_date <= '2020-07-24' &&"
        "creation_date >= '2020-03-26' &&"
        "is_reply_to_blm = TRUE"
    )

    # Get all tweets from the dataset first
    tweetsRecords: list[Record] = pb.client.collection("tweets_v2").get_full_list(
        query_params={"filter": filter_params}
    )

    # Get all users that exist in the dataset
    # Then get their tweets
    for record in tqdm(
        tweetsRecords, desc="Fetching user tweets", unit="user", ncols=100
    ):
        tweet = Tweet(**record.__dict__)

        user_id: str = tweet.user_id
        retrieved_user: Record = pb.get_user_by_id(user_id)
        user: User = User(**retrieved_user.__dict__)

        username: str = user.username
        number_of_tweets: int = user.number_of_tweets

        if (user.status == "fetched") or (user.status == "fetching"):
            logger.info(f"User '{username}' already has fetched tweets. Skipping.")
            continue

        if user.number_of_tweets > 5000:
            logger.info(
                f"User {username} has more than 5000 tweets ({number_of_tweets}). "
                "Using Tweety to fetch tweets."
            )
            continue

        logger.info(
            f"Fetching tweets for user: {username} (ID: {user_id}) - {number_of_tweets} tweets)"
        )

        pb.client.collection("tweet_users").update(
            retrieved_user.id, {"status": "fetching"}
        )

        tweets = get_user_tweets(
            user_id,
            username,
            Settings.OLD_BIRD_USERS_CONTINUATION_TOKEN.value,
            max_requests=max_requests
            if max_requests
            else number_of_tweets // TWEETS_PER_PAGE,
            api_key=Settings.X_RAPIDAPI_KEY.value,
        )
        logger.info(f"Total tweets fetched for {username}: {len(tweets)}")

        try:
            for tweet in tweets:
                json_filename = SAVE_DIR / f"{tweet['tweet_id']}.json"
                with open(json_filename, "w", encoding="utf-8") as f:
                    json.dump(tweet, f, ensure_ascii=False, indent=4)
        except Exception as e:
            logger.error(f"Error saving tweets for user {username}: {e}")
            continue

        pb.client.collection("tweet_users").update(
            retrieved_user.id,
            {"status": "fetched"},
        )
        logger.info(f"Updated user {username} with {len(tweets)} tweets.")


@cli.command()
@function_logger(LOGGER_DIR=LOGGER_DIR)
def get_all_users_tweets_by_tweety(
    max_pages: int | None = None,
    wait_time: int = 30,
    previous_session: bool | None = None,
) -> None:
    pb = PBWarehouse()
    scraper = TweetyScraper(
        previous_session=previous_session
        if previous_session is not None
        else (PROJECT_ROOT / "session.tw_session").exists()
    )
    SAVE_DIR = ensure_path(INTERIM_DATA_DIR / "tweety")
    TWEETS_PER_PAGE = 20

    have_data = True
    while have_data:
        try:
            userRecord: Record = pb.get_user_with_not_fetched_tweets()
            user: User = User(**userRecord.__dict__)
            max_pages = math.ceil(user.number_of_tweets / TWEETS_PER_PAGE)

            pb.client.collection("tweet_users").update(
                userRecord.id, {"status": "fetching"}
            )
            logger.info(
                f"Fetching tweets for user: {user.username} "
                f"(ID: {user.user_id}), tweets: {user.number_of_tweets}, pages: {max_pages}."
            )

            # Fetch tweets for the user
            tweets: list[dict] = asyncio.run(
                scraper.get_tweets_of_user(
                    username=user.username,
                    pages=max_pages if max_pages else max_pages,
                    wait_time=wait_time,
                )
            )

            if not tweets:
                logger.warning(
                    f"No tweets found for user {user.username}. "
                    f"Possibly because the tweets are sensitive."
                )
                pb.client.collection("tweet_users").update(
                    userRecord.id, {"status": "not fetched"}
                )
                continue

            logger.info(
                f"Fetched {len(tweets)} tweets for user {user.username} (ID: {user.user_id})"
            )

            # Save tweets to JSON files
            json_filename = SAVE_DIR / f"{user.username}.json"
            with open(json_filename, "w", encoding="utf-8") as f:
                json.dump(tweets, f, ensure_ascii=False, indent=4)

            pb.client.collection("tweet_users").update(
                userRecord.id,
                {
                    "status": "fetched",
                },
            )
            logger.info(f"Updated user {user.username} with {len(tweets)} tweets.")
        except ClientResponseError as e:
            if "The requested resource wasn't found." in str(e):
                logger.info("No more users to fetch tweets for.")
                pb.client.collection("tweet_users").update(
                    userRecord.id, {"status": "not fetched"}
                )
                have_data = False
            else:
                logger.error(f"ClientResponseError: {e}")
                pb.client.collection("tweet_users").update(
                    userRecord.id, {"status": "not fetched"}
                )
                have_data = False
        except Exception as e:
            logger.error(f"Error fetching tweets: {type(e).__name__} - {e}")
            have_data = False
            pb.client.collection("tweet_users").update(
                userRecord.id, {"status": "not fetched"}
            )


def grid_search(script: str = "hatebert") -> None:
    """Run grid search for hyperparameter tuning."""
    logger.add(PROJECT_ROOT / "reports" / "logs" / "grid_search.logs")
    logger.info(f"Running grid search for script: {script}")

    if script == "hatebert":
        for threshold in range(1, 10):
            subprocess.run(
                [
                    "uv",
                    "run",
                    "hatebert_training.py",
                    "--threshold",
                    str(threshold / 10),
                ],
            )
        logger.info("Completed grid search.")
    else:
        logger.error(f"Unknown script: {script}. Please use 'hatebert'.")


@cli.command()
def update_has_replies_using_reply_id() -> None:
    """Update the has_replies field for tweets."""
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
    """Clean the fetching buffer for users."""
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
    """Reset the fetched_tweets field for all users."""
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
    """Update the has_replies field for tweets."""
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
    """Update the in_reply_to_status_link field for tweets."""
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
def get_replies() -> None:
    """Get replies to tweets from the Oldbird API."""
    logger.add(PROJECT_ROOT / "reports" / "logs" / "get_replies.logs")
    logger.info("Starting to fetch replies from Oldbird API...")

    pb = PBWarehouse()
    staging_area = INTERIM_DATA_DIR / "oldbird"
    logger.info(f"Staging area: {staging_area}")

    filter_params = (
        "reply_count > 0 && "
        "has_blm_hashtag = TRUE &&"
        "creation_date <= '2020-07-24' &&"
        "creation_date >= '2020-03-26' &&"
        "fetched_replies = FALSE"
        # "is_reply_to_blm = TRUE"
    )

    records: list[Record] = pb.client.collection("tweets_v2").get_full_list(
        query_params={"filter": filter_params}
    )

    tweets: list[Tweet] = [Tweet(**r.__dict__) for r in records]
    tweet_ids: list[str] = [tweet.tweet_id for tweet in tweets]

    get_tweet_replies(tweet_ids, INTERIM_DATA_DIR / "oldbird")

    for tweet_id in tqdm(tweet_ids, desc="Updating tweets", unit="tweet", leave=False):
        try:
            pb.update_has_fetched_replies(tweet_id)
        except Exception as e:
            logger.error(f"Error updating tweet {tweet_id}: {type(e).__name__} - {e}")
            continue

    logger.info(f"Total tweets with replies: {len(tweets)}")
    logger.success("Replies fetched successfully.")


@cli.command()
@function_logger(LOGGER_DIR=LOGGER_DIR, level="WARNING")
def ingest_data() -> None:
    """Ingest data from stagign area to warehouse."""
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


@cli.command()
def get_from_oldbird(
    num_requests: int = Option(
        100, "--num-requests", "-n", help="Number of requests to make"
    ),
    continuation_token: str = Option(
        None, "--continuation-token", "-c", help="Optional continuation token"
    ),
):
    """Grab tweets from the Oldbird API and save them to a staging area."""
    logger.add(PROJECT_ROOT / "reports" / "logs" / "oldbird.logs")
    logger.info("Starting to fetch tweets from Oldbird API...")
    staging = INTERIM_DATA_DIR / "oldbird"
    token_file = staging / "continuation_token.txt"

    continuation_token = Settings.OLD_BIRD_CONTINUATION_TOKEN.value

    logger.info(f"Using continuation token: {continuation_token}")

    YEARS = range(2020, 2021)
    MONTHS = range(3, 6)

    for year in YEARS:
        for month in MONTHS:
            # year: int = 2020
            # month: int = 6
            last_day: int = calendar.monthrange(year, month)[1]
            for day in range(1, last_day + 1):
                querystring = {
                    "query": "#blacklivesmatter OR #blm",
                    "start_date": f"{year}-{month:02d}-{day:02d}",
                    "end_date": f"{year}-{month:02d}-{day + 1:02d}",
                    # "end_date": f"{year}-{month:02d}-{last_day:02d}",
                    "language": "en",
                    "min_retweets": "0",
                    "limit": "20",
                    "continuation_token": continuation_token,
                }

                def get_tweets(querystring, num_requests=5):
                    url = "https://twitter154.p.rapidapi.com/search/search/continuation"
                    headers = {
                        "x-rapidapi-key": Settings.X_RAPIDAPI_KEY.value,
                        "x-rapidapi-host": "twitter154.p.rapidapi.com",
                    }

                    querystring_cp = querystring.copy()

                    for _ in tqdm(
                        range(num_requests),
                        desc=f"Fetching tweets {year}-{month}-{day}",
                        unit="request",
                        ncols=100,
                    ):
                        response = requests.get(
                            url, headers=headers, params=querystring_cp
                        )
                        data = response.json()

                        if response.status_code != 200:
                            logger.error(
                                f"Error fetching data: {response.status_code} - {data.get('message', 'No message')}"
                            )
                            continue

                        if "results" not in data:
                            logger.error("No results found in the response")
                            continue

                        results = data["results"]

                        if len(results) == 0:
                            logger.warning("No tweets found in this request.")
                            break

                        for tweet in results:
                            json_filename = staging / f"{tweet['tweet_id']}.json"
                            with open(json_filename, "w", encoding="utf-8") as f:
                                json.dump(tweet, f, ensure_ascii=False, indent=4)

                        if "continuation_token" not in data:
                            logger.info(
                                "No continuation token found, stopping further requests."
                            )
                            break

                        with open(token_file, "w") as f:
                            f.write(data["continuation_token"])

                        querystring_cp["continuation_token"] = data[
                            "continuation_token"
                        ]

                get_tweets(querystring, num_requests=num_requests)

                tweet_list = list(staging.iterdir())
                logger.info(f"Total tweets fetched: {len(tweet_list) - 1}")


@cli.command()
@function_logger(LOGGER_DIR=LOGGER_DIR, level="WARNING")
def classify_data() -> None:
    """Classify data using HateBERT."""
    from transformers import AutoModelForSequenceClassification, AutoTokenizer

    pb = PBWarehouse()

    model_name = "Hate-speech-CNERG/bert-base-uncased-hatexplain"

    tokenizer = AutoTokenizer.from_pretrained(model_name)
    model = AutoModelForSequenceClassification.from_pretrained(model_name)

    def classify_text(text: str) -> int:
        inputs = tokenizer(text, return_tensors="pt", truncation=True)
        outputs = model(**inputs)
        logits = outputs.logits
        probs = torch.nn.functional.softmax(logits, dim=-1)
        predicted_class = torch.argmax(probs, dim=-1)
        return predicted_class.item()

    have_data = True

    try:
        while have_data:
            tweetRecord = pb.get_tweet_with_no_classification()
            tweet = Tweet(**tweetRecord.__dict__)
            logger.info(f"Classifying tweet: {tweet.tweet_id}")
            text_class = classify_text(tweet.text)
            pb.client.collection("tweets_v2").update(
                tweetRecord.id,
                {
                    "is_hateful": text_class,
                },
            )
            logger.success(
                f"Classified tweet {tweet.tweet_id} with class {text_class}."
            )
    except ClientResponseError as e:
        if "The requested resource wasn't found." in str(e):
            logger.info("No more tweets to classify.")
            have_data = False
    except Exception as e:
        logger.error(f"Error classifying tweet: {type(e).__name__} - {e}")
        have_data = False
        return


@cli.command()
def tweety_trends() -> None:
    """Run the Tweety script."""
    scraper = TweetyScraper(previous_session=True)
    asyncio.run(scraper.get_blm_trends())


@cli.command()
@function_logger(LOGGER_DIR=LOGGER_DIR)
def install_torch_geometric_dependencies() -> None:
    """Install necessary dependencies for PyTorch Geometric."""
    import torch

    torch_version = str(torch.__version__)
    scatter_src = f"https://pytorch-geometric.com/whl/torch-{torch_version}.html"
    sparse_src = f"https://pytorch-geometric.com/whl/torch-{torch_version}.html"

    subprocess.run(
        ["uv", "pip", "install", "torch-scatter", "-f", scatter_src],
        check=True,
    )
    subprocess.run(
        ["uv", "pip", "install", "torch-sparse", "-f", sparse_src],
        check=True,
    )


if __name__ == "__main__":
    cli()
