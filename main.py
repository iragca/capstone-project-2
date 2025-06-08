import asyncio
import json

from itertools import count
import polars as pl
from tqdm import tqdm
from tweety import TwitterAsync
from tweety.exceptions import ActionRequired
from typer import Typer

from src.config import (
    EXTERNAL_DATA_DIR,
    INTERIM_DATA_DIR,
    PROJECT_ROOT,
    Settings,
    logger,
)
from src.scraper import RapidApi

cli = Typer()


@cli.command()
def tweety() -> None:
    """Run the Tweety script."""
    # asyncio.run(tweety())
    pass


@cli.command()
def rapidapi(start: int = 0, step: int = 250, max_requests: int | None = None) -> None:
    """Run the RapidAPI script."""
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
