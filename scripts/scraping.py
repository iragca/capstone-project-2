import asyncio
import calendar
import json
import math

import requests
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
from src.scraper import RapidApiScraper, TweetyScraper
from src.utils import ensure_path, function_logger, get_tweet_replies, get_user_tweets

cli = Typer()


@cli.command()
@function_logger(LOGGER_DIR=LOGGER_DIR)
def tweety_login_once() -> None:
    """Log in to Twitter once. To generate your session token file."""
    scraper = TweetyScraper(previous_session=False)
    asyncio.run(scraper.login())


@cli.command()
@function_logger(LOGGER_DIR=LOGGER_DIR, level="WARNING")
def get_info_of_users():
    """Get the information of users in the PocketBase warehouse."""
    pb = PBWarehouse()
    scraper = RapidApiScraper(api_key=Settings.X_RAPIDAPI_KEY.value)
    SAVE_DIR = ensure_path(INTERIM_DATA_DIR / "twitter_api45_user")
    userRecords = pb.client.collection("users_tweets_status").get_full_list(
        query_params={"filter": "friends = 0"}
    )

    for user in tqdm(userRecords, desc="Fetching user info", unit="user", ncols=100):
        retrieved_user = scraper.get_user_info_by_twitter_api45(
            user.username, user.user_id
        )

        if not retrieved_user:
            logger.warning(f"User {user.username} (ID: {user.user_id}) not found.")
            continue

        with open(SAVE_DIR / f"{user.user_id}.json", "w", encoding="utf-8") as f:
            json.dump(retrieved_user, f, ensure_ascii=False, indent=4)

        logger.success(f"Fetched info for user {user.username} (ID: {user.user_id}).")


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

    YEARS = range(2024, 2025)
    MONTHS = range(1, 13)

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


if __name__ == "__main__":
    cli()
