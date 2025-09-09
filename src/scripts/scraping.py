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
    """
    Log in to Twitter once to generate a session token file.

    This command initializes a TweetyScraper instance without using a previous
    session and runs the login process asynchronously. It is intended to be run
    once to generate and store the session token locally for reuse in future
    scraping tasks.

    Parameters
    ----------
    None

    Returns
    -------
    None
        This function is executed for its side effect of creating a session token
        file. It does not return a value.

    Notes
    -----
    The session token file will be saved in the configured location for
    authentication reuse. Running this command again will overwrite the
    existing session token.

    See Also
    --------
    TweetyScraper.login : Method used internally to perform the login.
    """
    scraper = TweetyScraper(previous_session=False)
    asyncio.run(scraper.login())


@cli.command()
@function_logger(LOGGER_DIR=LOGGER_DIR, level="WARNING")
def get_info_of_users():
    """
    Fetch and store Twitter user information for users in the PocketBase warehouse.

    This command queries the `users_tweets_status` collection in the PocketBase
    database for users with `friends = 0`. For each user, it retrieves detailed
    profile information using the RapidAPI Twitter API v4.5, and saves the results
    as JSON files in the interim data directory.

    The function logs progress, warnings for missing users, and success messages
    for retrieved users.

    Parameters
    ----------
    None

    Returns
    -------
    None
        The function performs side effects:
        - Fetches user data from RapidAPI.
        - Saves results to the local filesystem in JSON format.
        - Logs progress and errors.

    Raises
    ------
    requests.exceptions.RequestException
        If there is a network or API request failure.
    ValueError
        If the API response is malformed or missing required fields.
    OSError
        If writing the JSON file to disk fails.

    Notes
    -----
    - Data is stored under ``INTERIM_DATA_DIR/twitter_api45_user``.
    - Each user record is saved as ``<user_id>.json``.
    - Uses tqdm for progress visualization.

    See Also
    --------
    RapidApiScraper.get_user_info_by_twitter_api45 : Retrieves a single user's info.
    PBWarehouse : Provides access to PocketBase collections.

    Examples
    --------
    Run the command via CLI:

    >>> get_info_of_users()
    Fetching user info: 100%|██████████| 25/25 [00:03<00:00,  7.90user/s]
    """
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
    """
    Fetch tweets for users stored in the PocketBase warehouse using the
    RapidAPI Twitter API v4.5.

    The function continuously queries PocketBase for users whose tweets
    have not been fetched yet (or have fewer than a given number of tweets),
    retrieves their tweets through the RapidAPI scraper, saves the results
    to disk as JSON files, and updates the user's fetch status in PocketBase.

    Parameters
    ----------
    max_retries : int, optional, default=5
        Maximum number of retries when fetching tweets for a user. Useful when
        the API prematurely reports no more tweets but additional tweets exist.
    less_than_k_tweets : int or None, optional
        If provided, only fetch tweets for users with fewer than `k` tweets.
    max_pages : int or None, optional
        Maximum number of pages to fetch from the API. If None, fetches all
        available pages.

    Returns
    -------
    None
        The function performs side effects:
        - Fetches tweets from RapidAPI.
        - Saves tweets to JSON files in the interim data directory.
        - Updates user records in PocketBase.
        - Logs progress, warnings, and errors.

    Raises
    ------
    aiohttp.ClientResponseError
        If the RapidAPI request fails with an HTTP error.
    OSError
        If saving tweets to disk fails.
    Exception
        Catches and logs all other unexpected errors during execution.

    Notes
    -----
    - Tweet data is saved in ``INTERIM_DATA_DIR/twitter_api45`` with filenames
      corresponding to the tweet IDs (``<tweet_id>.json``).
    - Each user record status in PocketBase is updated as:
        * ``fetching`` while being processed.
        * ``fetched`` if tweets were retrieved or determined to be sensitive.
        * ``not fetched`` if an error occurs.

    See Also
    --------
    RapidApiScraper.get_users_tweets_by_twitter_api45 : Retrieves tweets for a single user.
    PBWarehouse.get_user_with_not_fetched_tweets : Returns the next user to process.

    Examples
    --------
    Run the command via CLI with default options:

    >>> get_user_tweets_v2()

    Fetch tweets for users with fewer than 100 tweets:

    >>> get_user_tweets_v2(less_than_k_tweets=100)
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
    """
    Fetch all tweets for users in the OldBird dataset and store them locally.

    This function retrieves tweets from the PocketBase collection ``tweets_v2``
    within a specific date range and filters for tweets marked as replies to BLM.
    For each user associated with these tweets, it checks their fetch status
    and retrieves additional tweets via the OldBird API, saving the results to
    disk as JSON files. User statuses in PocketBase are updated accordingly.

    Parameters
    ----------
    max_requests : int or None, optional
        Maximum number of requests to make to the API for each user.
        If None, the number of requests is derived from the user's
        total tweet count and the page size (20 tweets per page).

    Returns
    -------
    None
        The function performs side effects:
        - Fetches tweets from OldBird API.
        - Saves tweets to JSON files in the interim data directory.
        - Updates user records in PocketBase.
        - Logs progress, skips, and errors.

    Raises
    ------
    OSError
        If saving tweets to disk fails.
    Exception
        If an unexpected error occurs while processing users or saving data.

    Notes
    -----
    - Data is saved in ``INTERIM_DATA_DIR/oldbird`` with filenames
      corresponding to tweet IDs (``<tweet_id>.json``).
    - Users with more than 5000 tweets are skipped, with a note to
      fetch their tweets using Tweety instead.
    - User statuses in PocketBase are updated as:
        * ``fetching`` while tweets are being retrieved.
        * ``fetched`` once tweets are successfully stored.
        * unchanged if skipped.

    See Also
    --------
    get_user_tweets : Helper function to fetch tweets for a single user.
    PBWarehouse.get_user_by_id : Retrieves user records from PocketBase.

    Examples
    --------
    Run with default options:

    >>> get_all_users_tweets_by_oldbird()

    Limit the maximum requests per user to 50:

    >>> get_all_users_tweets_by_oldbird(max_requests=50)
    """

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
    """
    Fetch all tweets for users using the Tweety scraper and store them locally.

    This function retrieves users from the PocketBase warehouse whose tweets
    have not yet been fetched. It then uses the Tweety scraper to download
    their tweets, saving them into JSON files in the staging directory. User
    statuses in PocketBase are updated throughout the process.

    Parameters
    ----------
    max_pages : int or None, optional
        Maximum number of pages of tweets to fetch per user. If None,
        the number of pages is computed based on the user's total tweet
        count and the page size (20 tweets per page).
    wait_time : int, default=30
        Number of seconds to wait between page requests to avoid rate
        limits or throttling by Twitter.
    previous_session : bool or None, optional
        Whether to reuse a previous Tweety session. If None, the function
        checks for the existence of a saved session file
        (``session.tw_session`` in the project root).

    Returns
    -------
    None
        The function performs side effects:
        - Fetches tweets using Tweety.
        - Saves tweets into JSON files under ``INTERIM_DATA_DIR/tweety``.
        - Updates user records in PocketBase.
        - Logs progress and errors.

    Raises
    ------
    ClientResponseError
        If a request to PocketBase fails or a user record cannot be updated.
    Exception
        If any unexpected error occurs during tweet retrieval or saving.

    Notes
    -----
    - Tweets are saved in JSON files named after the user's username
      (``<username>.json``).
    - User statuses in PocketBase are updated as:
        * ``fetching`` while tweets are being downloaded.
        * ``fetched`` once tweets are successfully stored.
        * ``not fetched`` if no tweets were found or an error occurred.
    - If a user has sensitive tweets, they may be skipped.

    See Also
    --------
    TweetyScraper.get_tweets_of_user : Internal scraper method for fetching tweets.
    get_all_users_tweets_by_oldbird : Alternative tweet collection using OldBird.

    Examples
    --------
    Run with default options:

    >>> get_all_users_tweets_by_tweety()

    Limit to 10 pages of tweets per user and reduce wait time:

    >>> get_all_users_tweets_by_tweety(max_pages=10, wait_time=10)
    """
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
    """
    Fetch replies to tweets from the Oldbird API and update the PocketBase warehouse.

    This function retrieves tweets from PocketBase that meet specific criteria
    (tweets with replies, containing BLM hashtags, within a given date range,
    and not yet marked as having fetched replies). It then fetches replies for
    those tweets using the Oldbird API and updates the corresponding records
    in PocketBase.

    Parameters
    ----------
    None

    Returns
    -------
    None
        The function performs side effects:
        - Fetches replies from the Oldbird API.
        - Saves replies in the staging area under ``INTERIM_DATA_DIR/oldbird``.
        - Updates PocketBase records to mark replies as fetched.
        - Logs progress, errors, and completion status.

    Raises
    ------
    Exception
        If updating a tweet record in PocketBase fails.

    Notes
    -----
    - Only tweets that satisfy the following criteria are processed:
        * ``reply_count > 0``
        * ``has_blm_hashtag = TRUE``
        * ``creation_date`` between 2020-03-26 and 2020-07-24
        * ``fetched_replies = FALSE``
    - Replies are saved locally in the ``oldbird`` staging directory.
    - Each processed tweet is marked in PocketBase to prevent duplicate fetching.

    See Also
    --------
    get_all_users_tweets_by_oldbird : Function for fetching user tweets via Oldbird.
    get_all_users_tweets_by_tweety : Function for fetching tweets via Tweety.

    Examples
    --------
    Run with default options:

    >>> get_replies()
    """
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
    """
    Fetch tweets from the Oldbird API using RapidAPI and save them to a staging area.

    This function iterates over months and days within a defined year range,
    constructs queries with specific filters, and retrieves tweets from the
    Oldbird API via RapidAPI. Tweets are saved as individual JSON files in a
    staging directory, and continuation tokens are managed for paginated requests.

    Parameters
    ----------
    num_requests : int, optional
        Maximum number of requests to make per day (default is 100).
    continuation_token : str, optional
        A continuation token used to resume fetching tweets from a specific point.
        If not provided, the default from settings is used.

    Returns
    -------
    None
        The function performs side effects:
        - Fetches tweets and saves them as JSON files under `INTERIM_DATA_DIR/oldbird`.
        - Writes the latest continuation token to a file (`continuation_token.txt`).
        - Logs progress, warnings, and errors.

    Raises
    ------
    requests.RequestException
        If an HTTP request to the API fails.
    ValueError
        If the API response does not contain expected fields.

    Notes
    -----
    - Currently configured to fetch tweets for years 2024–2024 (inclusive).
    - Each day's tweets are fetched separately with pagination.
    - The continuation token is updated after each request and persisted
      for subsequent runs.
    - Results are rate-limited by `num_requests`.

    See Also
    --------
    get_all_users_tweets_by_oldbird : Fetch user-specific tweets from Oldbird.
    get_replies : Fetch replies for previously collected tweets.

    Examples
    --------
    Run with defaults:

    >>> get_from_oldbird()

    Run with custom number of requests:

    >>> get_from_oldbird(num_requests=50)

    Resume with a continuation token:

    >>> get_from_oldbird(continuation_token="abcd1234")
    """

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
