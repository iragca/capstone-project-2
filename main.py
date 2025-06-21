import asyncio
import json
from itertools import count
from pprint import pprint

import polars as pl
from tqdm import tqdm
from typer import Typer

from src.config import (
    EXTERNAL_DATA_DIR,
    INTERIM_DATA_DIR,
    PROJECT_ROOT,
    Settings,
    logger,
)
from src.db import DB
from src.scraper import RapidApi, TweetyScraper

cli = Typer()


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
