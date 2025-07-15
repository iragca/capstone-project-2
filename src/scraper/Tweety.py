from pprint import pprint
from time import sleep

import polars as pl
from tqdm import tqdm
from tweety import TwitterAsync
from tweety.types import Search, SelfThread
from tweety.types import Tweet as TweetyTweet

from src.config import EXTERNAL_DATA_DIR, logger
from src.config import Settings as s
from src.db import DB
from src.models import Tweet


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

    async def get_data(self, delay: int = 10) -> None:
        app: TwitterAsync = await self.login()
        data: list[str] = self.load_blm_data().to_numpy().reshape(-1).tolist()
        db = DB()

        columns = [
            "id",
            "text",
            "retweet_count",
            "reply_count",
            "like_count",
            "quote_count",
            "community_note",
            "comments",
            "in_reply_to",
            "sensitive_flag",
            "lang",
            "time_of_day",
            "is_reply",
            "source",
            "url",
            "author_id",
            "author_name",
            "verified_author",
            "bookmark_count",
            "views",
            "has_moderated_replies",
            "community",
        ]

        for tweet_id in tqdm(data, desc="Fetching tweets", unit="tweet"):
            sleep(delay)
            try:
                tweet: Tweet = await app.tweet_detail(tweet_id)
                author = tweet.author

                record = {
                    "id": tweet.id,
                    "text": tweet.text,
                    "retweet_count": tweet.retweet_counts,
                    "reply_count": tweet.reply_counts,
                    "like_count": tweet.likes,
                    "quote_count": tweet.quote_counts,
                    "community_note": tweet.community_note,
                    "comments": tweet.comments,
                    "in_reply_to": tweet.replied_to,
                    "sensitive_flag": tweet.is_sensitive,
                    "lang": tweet.language,
                    "time_of_day": tweet.created_on,
                    "is_reply": tweet.is_reply,
                    "source": tweet.source,
                    "url": tweet.url,
                    "author_id": author.id if author else None,
                    "author_name": author.username if author else None,
                    "verified_author": author.verified if author else None,
                    "bookmark_count": tweet.bookmark_count,
                    "views": tweet.views,
                    "has_moderated_replies": tweet.has_moderated_replies,
                    "community": tweet.community,
                }

                insert_data = tuple(record[col] for col in columns)
                # pprint(insert_data)

                db.execute(
                    f"""
                    INSERT INTO tweets (
                        {", ".join(columns)}
                    ) VALUES (
                        {", ".join(["?"] * len(columns))}
                    )
                    """,
                    insert_data,
                )
                db.connection.commit()

            except Exception as e:
                db.execute(
                    "INSERT INTO errors (id, error_message, error_type) VALUES (?, ?, ?)",
                    (tweet_id, str(e), type(e).__name__),
                )
                db.connection.commit()
                logger.error(f"Error processing tweet {tweet_id}: {e}")

    def load_blm_data(self) -> pl.DataFrame:
        BLM_DATA: pl.DataFrame = pl.read_csv(
            EXTERNAL_DATA_DIR / "TAPS dataset" / "blacklivesmatter.txt",
            schema={"tweetIds": pl.Utf8},
        )

        return BLM_DATA

    async def get_blm_trends(self) -> None:
        app: TwitterAsync = await self.login()

        data: Search = await app.search("#blacklivesmatter", pages=10, wait_time=10)

        data.to_xlsx()

    async def get_tweets_of_user(self, username: str, pages: int = 100) -> list[dict]:
        app: TwitterAsync = await self.login()

        tweets: list[TweetyTweet] = await app.get_tweets(
            username, wait_time=60, pages=pages
        )

        if not tweets:
            logger.warning(f"No tweets found for user {username}.")
            return []

        if len(tweets) == 0:
            logger.warning(f"No tweets found for user {username}.")
            return []

        validated_tweets = []

        try:
            for tweet in tweets:
                if isinstance(tweet, TweetyTweet):
                    tweet_data = self.process_tweety_tweet(tweet)
                    validated_tweets.append(tweet_data)
                elif isinstance(tweet, SelfThread):
                    for t in tweet.tweets:
                        assert isinstance(t, TweetyTweet), (
                            f"Expected TweetyTweet type, got {type(t)}"
                        )
                        tweet_data = self.process_tweety_tweet(t)
                        validated_tweets.append(tweet_data)
                else:
                    logger.warning(f"Unknown tweet type: {type(tweet)}")
        except KeyboardInterrupt:
            logger.info("Scraping interrupted by user.")
            exit(0)
        except Exception as e:
            logger.error(f"Error processing tweets for user {username}: {e}")

        return validated_tweets

    @staticmethod
    def process_tweety_tweet(tweet: TweetyTweet) -> dict:
        search_terms = [
            "#blacklivesmatter",
            "#blm",
            "#blacklivesmatters",
        ]

        conversation_id = tweet.__dict__.get("_original_tweet", {}).get(
            "conversation_id_str", None
        )

        if not conversation_id:
            pprint(tweet.__dict__)
            raise ValueError(
                f"Conversation ID not found in tweet {tweet.id}. Please check the tweet structure."
            )

        views = tweet.views if isinstance(tweet.views, int) else None

        tweet = Tweet(
            tweet_id=tweet.id,
            text=tweet.text,
            status_link=tweet.url,
            user_id=tweet.author.id,
            in_reply_to_status_id=tweet.replied_to,
            bookmark_count=tweet.bookmark_count,
            views=views,
            retweet_count=tweet.retweet_counts,
            favorite_count=tweet.likes,
            reply_count=tweet.reply_counts,
            quote_count=tweet.quote_counts,
            conversation_id=conversation_id,
            retweet_status_id=tweet.retweeted_tweet.id
            if tweet.retweeted_tweet
            else None,
            quoted_status_id=tweet.quoted_tweet.id if tweet.quoted_tweet else None,
            community_note=tweet.community_note,
            language=tweet.language,
            source=tweet.source,
            creation_date=str(tweet.created_on),
            has_blm_hashtag=any(term in tweet.text.lower() for term in search_terms),
        )

        return tweet.model_dump()
