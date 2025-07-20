from typing import Optional

from pydantic import BaseModel
from typing import Literal


class User(BaseModel):
    user_id: str
    username: Optional[str]
    name: Optional[str]
    follower_count: int
    following_count: int
    favourites_count: int
    listed_count: int
    number_of_tweets: int
    is_private: Optional[bool]
    is_verified: Optional[bool]
    is_blue_verified: bool
    bot: bool
    location: Optional[str]
    description: str
    status: Literal["fetched", "not fetched", "fetching", ""] = "not fetched"
    creation_date: Optional[str]
    friends: Optional[int] = 0
