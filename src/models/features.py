from pydantic import BaseModel, model_validator
from typing import Literal


class Features(BaseModel):
    """
    Base class for node types.
    This class can be extended to define specific node types.
    """

    tweet: list[
        Literal[
            "favorite_count",
            "bookmark_count",
            "retweet_count",
            "reply_count",
            "quote_count",
            "views",
            "is_hateful",
            "source",
        ]
    ] = []

    user: list[
        Literal[
            "favourites_count",
            "follower_count",
            "following_count",
            "number_of_tweets",
            "listed_count",
            "is_blue_verified",
            "friends",
        ]
    ] = []
