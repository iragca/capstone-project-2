from typing import Optional

from pydantic import BaseModel


class User(BaseModel):
    creation_date: str
    user_id: str
    username: str
    name: str
    follower_count: int
    following_count: int
    favourites_count: int
    is_private: Optional[bool]
    is_verified: bool
    is_blue_verified: bool
    location: str
    description: str
    number_of_tweets: int
    bot: bool
    listed_count: int
