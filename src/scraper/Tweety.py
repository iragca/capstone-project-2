from pprint import pprint

from tweety import TwitterAsync
from tweety.filters import SearchFilters
from tweety.types import Search, SelfThread
from tweety.types import Tweet as TweetyTweet
from tweety.types import User as TweetyUser
from tweety.exceptions import UserNotFound

from src.config import Settings as s
from src.config import logger
from src.models import Tweet, User


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
            await app.sign_in(USERNAME, s.X_PASSWORD.value, extra=s.X_TOTP.value)

        return app

    async def get_blm_trends(self) -> None:
        app: TwitterAsync = await self.login()

        data: Search = await app.search("#blacklivesmatter", pages=10, wait_time=10)

        data.to_xlsx()

    async def get_tweets_of_user(
        self, username: str, pages: int = 100, wait_time: int = 30
    ) -> list[dict]:
        app: TwitterAsync = await self.login()

        tweets: list[TweetyTweet] = await app.search(
            f"(from:{username})",
            wait_time=wait_time,
            pages=pages,
            filter_=SearchFilters.Latest(),
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

    async def get_user_info(self, user_id: int | None, username: str | None) -> User:
        """
        Get user information by user ID.

        Args:
            user_id (str): The ID of the user to retrieve information for.

        Returns:
            dict: A dictionary containing user information.
        """
        try:
            if not user_id and not username:
                raise ValueError("Either User ID or Username must be provided.")

            user_id = str(user_id) if user_id is not None else None

            if username:
                if not isinstance(username, str):
                    raise ValueError("Username must be a string.")

            if user_id is not None:
                if not isinstance(user_id, int):
                    raise ValueError("User ID must be numeric.")

            app: TwitterAsync = await self.login()
            user_info = await app.get_user_info(user_id or username)

            return self.process_tweety_user(user_info)
        except UserNotFound:
            return None

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

    @staticmethod
    def process_tweety_user(user: TweetyUser) -> User:
        """
        Process a Tweety user object into a dictionary.

        Args:
            user (User): The user object to process.

        Returns:
            dict: A dictionary containing user information.
        """
        return User(
            user_id=user.id,
            username=user.username,
            name=user.name,
            follower_count=user.followers_count,
            following_count=user.following,
            favourites_count=user.favourites_count,
            listed_count=user.listed_count,
            number_of_tweets=user.statuses_count,
            is_private=user.protected,
            is_blue_verified=user.verified,
            location=user.location["location"] if user.location else None,
            description=user.description,
            status="not fetched",
            creation_date=str(user.created_at),
            friends=user.friends_count,
        )
