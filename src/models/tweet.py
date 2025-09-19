from __future__ import annotations

from typing import Literal, Optional

from pydantic import BaseModel

from .user import User


class Tweet(BaseModel):
    """
    A Pydantic model representing a tweet.

    Attributes:
        tweet_id (str): Unique identifier of the tweet.
        text (str): Raw tweet text content.
        status_link (str): Direct URL to the tweet.
        user_id (str): Identifier of the authoring user.
        is_extremist (Optional[bool], default=False): Whether the tweet is flagged as extremist.
        is_annotated (Optional[bool], default=False): Whether the tweet has been annotated.
        in_reply_to_status_link (Optional[str]): URL of the tweet this one replies to.
        in_reply_to_status_id (Optional[str]): ID of the tweet this one replies to.
        bookmark_count (int): Number of bookmarks.
        views (Optional[int]): Number of views (impressions).
        retweet_count (int): Number of retweets.
        favorite_count (int): Number of likes.
        reply_count (int): Number of replies.
        quote_count (int): Number of quote tweets.
        conversation_id (str): ID of the root conversation this tweet belongs to.
        retweet_status_id (Optional[str]): ID of the retweeted tweet if applicable.
        quoted_status_id (Optional[str]): ID of the quoted tweet if applicable.
        community_note (Optional[str]): Community note attached to the tweet.
        language (str): ISO language code of the tweet text.
        source (Optional[str]): Client/application used to post the tweet.
        creation_date (str): Timestamp of creation (ISO format recommended).
        has_blm_hashtag (bool): Whether the tweet contains a BLM-related hashtag.
        fetched_replies (Optional[bool], default=False): Whether replies have been fetched.
        is_reply_to_blm (Optional[bool]): Whether this is a reply to a BLM-related tweet.
        is_hateful (Optional[Literal["0","1","2",""]]): Hate speech classification.
            "0" = not hateful, "1" = mildly hateful, "2" = highly hateful, "" = unset.
    """

    tweet_id: str
    text: str
    status_link: str
    user_id: str | User
    is_extremist: Optional[bool] = False
    is_annotated: Optional[bool] = False
    in_reply_to_status_link: Optional[str] = None
    in_reply_to_status_id: Optional[str] = None
    bookmark_count: int
    views: Optional[int] = None
    retweet_count: int
    favorite_count: int
    reply_count: int
    quote_count: int
    conversation_id: str
    retweet_status_id: Optional[str] = None
    quoted_status_id: Optional[str] = None
    community_note: Optional[str] = None
    language: str
    source: Optional[str] = None
    creation_date: str
    has_blm_hashtag: bool
    fetched_replies: Optional[bool] = False
    is_reply_to_blm: Optional[bool] = None
    is_hateful: Optional[Literal["0", "1", "2", ""]] = None
