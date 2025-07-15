from typing import Optional

from pydantic import BaseModel


class User(BaseModel):
    user_id: str
    username: str
    name: str
    follower_count: int
    following_count: int
    favourites_count: int
    listed_count: int
    number_of_tweets: int
    is_private: Optional[bool]
    is_verified: bool
    is_blue_verified: bool
    bot: bool
    location: str
    description: str
    fetched_tweets: bool = False
    creation_date: str
