import json
import time

import torch
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
@function_logger(LOGGER_DIR=LOGGER_DIR, level="WARNING")
def update_user_friends():
    """Update the friends count for users in the PocketBase warehouse."""
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
@function_logger(LOGGER_DIR=LOGGER_DIR, level="WARNING")
def ingest_data() -> None:
    """Ingest data from staging area to warehouse."""
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
@function_logger(LOGGER_DIR=LOGGER_DIR, level="WARNING")
def classify_data(collection: str = "tweets_v2") -> None:
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
            tweetRecord = pb.get_tweet_with_no_classification(collection=collection)
            tweetRecord: Record = pb.client.collection("tweets_v2").get_one(
                tweetRecord.id
            )
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


if __name__ == "__main__":
    cli()
