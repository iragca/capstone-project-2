from pydantic import BaseModel
from .tweet import Tweet


class TweetLinkProbability(BaseModel):
    tweet: Tweet
    score: float
