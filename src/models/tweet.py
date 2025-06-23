from __future__ import annotations

from typing import Optional

from pydantic import BaseModel


class Tweet(BaseModel):
    tweet_id: str
    text: str
    user_id: str
    bookmark_count: int
    views: Optional[int]
    retweet_count: int
    favorite_count: int
    reply_count: int
    quote_count: int
    in_reply_to_status_id: Optional[str]
    conversation_id: str
    retweet_tweet_id: Optional[str]
    quoted_status_id: Optional[str]
    community_note: Optional[str]
    language: str
    source: Optional[str]
    creation_date: str
    has_blm_hashtag: bool
