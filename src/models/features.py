from pydantic import BaseModel
from typing import Literal


class Features(BaseModel):
    """
    Base class for specifying features of different node types.

    This model is designed to represent the attributes of tweet and user nodes
    in a graph or dataset. It can be extended or instantiated to define which
    features are relevant for analysis or modeling.

    Attributes
    ----------
    tweet : list of {"favorite_count", "bookmark_count", "retweet_count", "reply_count", \
                     "quote_count", "views", "is_hateful", "source"}, optional
        List of features associated with tweet nodes. Defaults to an empty list.
        - ``favorite_count`` : Number of likes a tweet has received.
        - ``bookmark_count`` : Number of times a tweet has been bookmarked.
        - ``retweet_count`` : Number of retweets.
        - ``reply_count`` : Number of replies.
        - ``quote_count`` : Number of quote tweets.
        - ``views`` : Number of views/impressions.
        - ``is_hateful`` : Boolean indicator if the tweet is hateful.
        - ``source`` : Source application of the tweet (e.g., "Twitter for iPhone").

    user : list of {"favourites_count", "follower_count", "following_count", "number_of_tweets", \
                    "listed_count", "is_blue_verified", "friends"}, optional
        List of features associated with user nodes. Defaults to an empty list.
        - ``favourites_count`` : Number of tweets the user has liked.
        - ``follower_count`` : Number of followers the user has.
        - ``following_count`` : Number of accounts the user follows.
        - ``number_of_tweets`` : Total number of tweets posted by the user.
        - ``listed_count`` : Number of public lists the user belongs to.
        - ``is_blue_verified`` : Boolean indicator of Twitter Blue verification status.
        - ``friends`` : Number of mutual connections (friends).

    Examples
    --------
    >>> features = Features(
    ...     tweet=["favorite_count", "retweet_count", "views"],
    ...     user=["follower_count", "is_blue_verified"]
    ... )
    >>> features.tweet
    ['favorite_count', 'retweet_count', 'views']
    >>> features.user
    ['follower_count', 'is_blue_verified']
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
