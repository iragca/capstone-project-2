from typing import Literal, Optional

from pydantic import BaseModel


class User(BaseModel):
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
