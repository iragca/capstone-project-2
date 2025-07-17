from pprint import pprint

from tweety import TwitterAsync
from tweety.types import Search, SelfThread
from tweety.types import Tweet as TweetyTweet

from src.config import logger
from src.config import Settings as s
from src.models import Tweet


class TweetyScraper:
    def __init__(self, previous_session: bool = True):
        self.previous_session = previous_session

    async def login(self) -> TwitterAsync:
        app: TwitterAsync = TwitterAsync("session")
        if self.previous_session:
            await app.connect()
        else:
            USERNAME: str = s.X_USERNAME.value
            PASSWORD: str = s.X_PASSWORD.value
            TOTP: int = s.X_TOTP.value

            assert USERNAME and PASSWORD and TOTP, (
                "Username, password, and TOTP must be provided in the settings."
            )
            await app.sign_in(
                USERNAME, s.X_PASSWORD.value, extra=s.X_TOTP.value
            )

        return app

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
