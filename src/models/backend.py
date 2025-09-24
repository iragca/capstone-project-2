from typing import List, Optional

from pydantic import BaseModel, Field

from .inference import TweetLinkProbability
from .user import User


class InferenceOptions(BaseModel):
    top_k: Optional[int] = Field(
        10, description="Number of top similar tweets to return", minimum=1, maximum=50
    )
    strict_matching: Optional[bool] = Field(
        True, description="Whether to use strict matching for user lookup"
    )
    descending: bool = Field(
        True, description="Whether to sort results in descending order of similarity"
    )


class InferenceRequest(BaseModel):
    username: str = Field(
        ..., example="elonmusk", description="Twitter handle of the user", min_length=1
    )
    user_id: Optional[str] = Field(
        None, example="44196397", description="Twitter ID of the user", min_length=1
    )
    options: Optional[InferenceOptions] = InferenceOptions()


class APIResponse(BaseModel):
    message: str


class InferenceAPIResponse(APIResponse):
    data: List[TweetLinkProbability]
    user: User


class GetUserAPIResponse(APIResponse):
    data: User
