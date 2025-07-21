from typing import Optional

from pydantic import BaseModel
from typing import Literal


class User(BaseModel):
    user_id: str
    username: Optional[str]
    name: Optional[str]
    follower_count: Optional[int] = 0
    following_count: Optional[int] = 0
    favourites_count: Optional[int] = 0
    listed_count: Optional[int] = 0
    number_of_tweets: Optional[int] = 0
    is_private: Optional[bool]
    is_verified: Optional[bool]
    is_blue_verified: Optional[bool]
    bot: Optional[bool]
    location: Optional[str]
    description: str
    status: Literal["fetched", "not fetched", "fetching", ""] = "not fetched"
    creation_date: Optional[str]
    friends: Optional[int] = 0
