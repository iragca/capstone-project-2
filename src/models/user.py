from typing import Literal, Optional

from pydantic import BaseModel


class User(BaseModel):
    """
    A Pydantic model representing a user profile.

    Attributes:
        user_id (str): Unique identifier of the user.
        username (Optional[str]): Username/handle of the user.
        name (Optional[str]): Display name of the user.
        follower_count (Optional[int], default=0): Number of followers.
        following_count (Optional[int], default=0): Number of accounts this user follows.
        favourites_count (Optional[int], default=0): Number of likes/favorites.
        listed_count (Optional[int], default=0): Number of lists this user is a member of.
        number_of_tweets (Optional[int], default=0): Number of tweets the user has posted.
        is_private (Optional[bool]): Whether the account is private.
        is_verified (Optional[bool]): Whether the account has a standard verification badge.
        is_blue_verified (Optional[bool]): Whether the account has a Twitter Blue verification.
        bot (Optional[bool]): Whether the account is identified as a bot.
        location (Optional[str]): User-provided location string.
        description (str): Profile description (bio).
        status (Literal["fetched","not fetched","fetching",""], default="not fetched"):
            Fetch status of this user’s profile.
        creation_date (Optional[str]): Date the account was created (ISO format preferred).
        friends (Optional[int], default=0): Number of “friends” (usually equivalent to following).
    """

    user_id: str
    username: Optional[str]
    name: Optional[str]
    follower_count: Optional[int] = 0
    following_count: Optional[int] = 0
    favourites_count: Optional[int] = 0
    listed_count: Optional[int] = 0
    number_of_tweets: Optional[int] = 0
    is_private: Optional[bool] = None
    is_verified: Optional[bool] = None
    is_blue_verified: Optional[bool] = None
    bot: Optional[bool] = None
    location: Optional[str] = None
    description: str
    status: Literal["fetched", "not fetched", "fetching", ""] = "not fetched"
    creation_date: Optional[str]
    friends: Optional[int] = 0
