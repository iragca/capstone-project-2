from pydantic import BaseModel


class TweetUser(BaseModel):
    user_id: str
