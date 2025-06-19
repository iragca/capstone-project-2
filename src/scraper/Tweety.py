from time import sleep

import polars as pl
from tqdm import tqdm
from tweety import TwitterAsync
from tweety.types import Tweet

from src.config import (
    EXTERNAL_DATA_DIR,
)
from src.config import Settings as s


class TweetyScraper:
    def __init__(self, previous_session: bool = True):
        self.previous_session = previous_session

    async def login(self) -> TwitterAsync:
        app: TwitterAsync = TwitterAsync("session")
        if self.previous_session:
            await app.connect()
        else:
            await app.sign_in(
                s.X_USERNAME.value, s.X_PASSWORD.value, extra=s.X_TOTP.value
            )

        return app

    async def get_data(self, delay: int = 1) -> None:
        app: TwitterAsync = await self.login()
        data: list[str] = self.load_data().to_numpy().reshape(-1).tolist()

        for tweet_id in tqdm(data, desc="Fetching tweets", unit="tweet"):
            sleep(delay)  # Respect rate limits
            try:
                tweet: Tweet = await app.tweet_detail(tweet_id)
                print(tweet.text)
            except Exception as e:
                print(f"Error fetching tweet {tweet_id}: {e}")

    def load_data(self) -> pl.DataFrame:
        BLM_DATA: pl.DataFrame = pl.read_csv(
            EXTERNAL_DATA_DIR / "TAPS dataset" / "blacklivesmatter.txt",
            schema={"tweetIds": pl.Utf8},
        )

        return BLM_DATA.sample(5)
