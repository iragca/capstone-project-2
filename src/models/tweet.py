from __future__ import annotations

from typing import Optional

from typing import Literal
from pydantic import BaseModel


class Tweet(BaseModel):
    tweet_id: str
    text: str
    status_link: str
    user_id: str
    is_extremist: Optional[bool] = False
    is_annotated: Optional[bool] = False
    in_reply_to_status_link: Optional[str] = None
    in_reply_to_status_id: Optional[str]
    bookmark_count: int
    views: Optional[int]
    retweet_count: int
    favorite_count: int
    reply_count: int
    quote_count: int
    conversation_id: str
    retweet_status_id: Optional[str]
    quoted_status_id: Optional[str]
    community_note: Optional[str]
    language: str
    source: Optional[str]
    creation_date: str
    has_blm_hashtag: bool
    fetched_replies: Optional[bool] = False
    is_reply_to_blm: Optional[bool] = None
    is_hateful: Optional[Literal["0", "1", "2", ""]] = None
