import asyncio
import calendar
import json
from itertools import count
from pprint import pprint

import polars as pl
import requests
from datetime import datetime
from pocketbase.models import Record
from tqdm import tqdm
from typer import Option, Typer

from src.config import (
    EXTERNAL_DATA_DIR,
    INTERIM_DATA_DIR,
    PROJECT_ROOT,
    Settings,
    logger,
)
from src.db import DB, PBWarehouse
from src.models import Tweet
from src.scraper import RapidApi, TweetyScraper
from src.utils import get_tweet_replies

cli = Typer()


@cli.command()
def update_migrations() -> None:
    logger.add(PROJECT_ROOT / "reports" / "logs" / "migrations.logs")

    pb = PBWarehouse()
    records: list[Record] = pb.client.collection("tweets").get_full_list()

    for record in tqdm(records, desc="Updating tweets", unit="tweet"):
        try:
            update_record = pb.client.collection("tweets_v2").get_list(
                1, 1, {"filter": f"tweet_id = '{record.tweet_id}'"}
            )

            if not update_record.items:
                logger.warning(
                    f"No record found for tweet_id {record.tweet_id}. Skipping."
                )
                continue
            pb.client.collection("tweets_v2").update(
                update_record.items[0].id,
                {
                    "is_annotated": record.is_annotated,
                    "is_extremist": record.is_extremist,
                },
            )
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
        # "creation_date <= '2020-07-24'"
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
def ingest_data() -> None:
    """Ingest data from stagign area to warehouse."""
    logger.add(PROJECT_ROOT / "reports" / "logs" / "ingest_data.logs")
    logger.info("Starting data ingestion process...")

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

                logger.info(f"Successfully ingested {tweet_file.name}")
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

    YEARS = range(2025, 2026)
    MONTHS = range(1, 7)

    for year in YEARS:
        for month in MONTHS:
            last_day: int = calendar.monthrange(year, month)[1]

            querystring = {
                "query": "#blacklivesmatter OR #blm",
                "start_date": f"{year}-{month:02d}-01",
                "end_date": f"{year}-{month:02d}-{last_day:02d}",
                "language": "en",
                "limit": "20",
                "continuation_token": continuation_token,
            }

            def get_tweets(querystring, num_requests=5):
                url = "https://twitter154.p.rapidapi.com/search/search/continuation"
                headers = {
                    "x-rapidapi-key": "3ba6bea96amsha13f50dd29c930fp1f1cf9jsnc15627770e18",
                    "x-rapidapi-host": "twitter154.p.rapidapi.com",
                }

                querystring_cp = querystring.copy()

                for _ in tqdm(range(num_requests), desc=f"Fetching tweets {year}-{month}", unit="request", ncols=100):
                    response = requests.get(url, headers=headers, params=querystring_cp)
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
                        logger.info("No continuation token found, stopping further requests.")
                        break

                    with open(token_file, "w") as f:
                        f.write(data["continuation_token"])

                    querystring_cp["continuation_token"] = data["continuation_token"]

            get_tweets(querystring, num_requests=num_requests)

            tweet_list = list(staging.iterdir())
            logger.info(f"Total tweets fetched: {len(tweet_list) - 1}")


@cli.command()
def tweety() -> None:
    """Run the Tweety script."""
    logger.add(PROJECT_ROOT / "reports" / "logs" / "tweet.logs")
    scraper = TweetyScraper(previous_session=True)
    asyncio.run(scraper.get_data())


@cli.command()
def tweety_login() -> None:
    """Login to Twitter using Tweety."""
    scraper = TweetyScraper(previous_session=False)
    asyncio.run(scraper.login())


@cli.command()
def tweety_trends() -> None:
    """Run the Tweety script."""
    scraper = TweetyScraper(previous_session=True)
    asyncio.run(scraper.get_blm_trends())


@cli.command()
def view_db_data() -> None:
    db = DB()
    db.view_data()
    db.close()


@cli.command()
def view_schema() -> None:
    """View the schema of the tweets table."""
    db = DB()
    schema = db.connection.execute("DESCRIBE tweets").fetchall()
    pprint(schema)


@cli.command()
def rapidapi_tweets(
    start: int = 0, step: int = 250, max_requests: int | None = None
) -> None:
    """Scrape tweets using RapidAPI using Tweet IDs from the BLM dataset."""

    logger.add(
        f"{PROJECT_ROOT}/logs/ingest_external_data.log",
        rotation="1 day",
        retention="7 days",
        level="INFO",
    )
    logger.info("Starting RapidAPI data scraping...")

    BLM_DATA = pl.read_csv(
        EXTERNAL_DATA_DIR / "TAPS dataset" / "blacklivesmatter.txt",
        schema={"tweetIds": pl.Utf8},
    ).unique()
    length = BLM_DATA.shape[0]

    ra = RapidApi(api_key=Settings.X_RAPIDAPI_KEY.value)

    logger.info(f"Total tweet IDs: {length:,}")
    logger.info(f"Tweets per request: {step}")
    logger.info(f"Total requests needed: {length / step:,}")

    counter = count(start=1)
    num_requests = 0

    for i in tqdm(range(start, length, step), desc="Scraping tweets", unit="request"):
        data: dict = ra.get_data(
            data=BLM_DATA,
            start=i,
            end=i + 250,
        )

        num_requests = next(counter)

        try:
            with open(
                INTERIM_DATA_DIR / "twttr" / f"blacklivesmatter_{i}:{i + 250}.json", "w"
            ) as f:
                json.dump(data, f, indent=4)
        except Exception as e:
            logger.error(f"{type(e).__name__} while saving data to JSON:", e)
            continue

        if max_requests is not None and num_requests > max_requests:
            logger.warning(
                f"Reached maximum requests limit: {max_requests}. Stopping further requests."
            )
            break

    logger.success("Data saved")
    logger.info(f"Requests made: {counter}")
    logger.info(f"Total tweets scraped: {num_requests * step}")


if __name__ == "__main__":
    cli()
