from pydantic import BaseModel
from src.models import Tweet


class TweetLinkProbability(BaseModel):
    tweet: Tweet
    score: float
