import json
from http.client import HTTPResponse, HTTPSConnection

from polars import DataFrame
from pydantic import BaseModel, Field, model_validator

from src.config import logger


class InputData(BaseModel):
    data: DataFrame
    start: int = Field(ge=0)
    end: int

    model_config = {"arbitrary_types_allowed": True}

    @model_validator(mode="after")
    def check_range(self) -> "InputData":
        if self.end - self.start > 250:
            raise ValueError("The range must not exceed 250 tweet IDs.")
        return self


class RapidApi(BaseModel):
    api_key: str = Field(min_length=50, max_length=50)
    api_host: str = Field(default="twitter241.p.rapidapi.com")

    def get_headers(self):
        return {
            "x-rapidapi-key": self.api_key,
            "x-rapidapi-host": self.api_host,
        }

    def get_data(self, data: DataFrame, start: int, end: int) -> dict:

        input = InputData(data=data, start=start, end=end)

        try:
            conn = HTTPSConnection("twitter241.p.rapidapi.com")

            tweet_ids: str = r"%2C".join(
                list(input.data["tweetIds"][input.start : input.end])
            )
            conn.request(
                "GET", "/tweet-by-ids?tweetIds=" + tweet_ids, headers=self.get_headers()
            )
            res: HTTPResponse = conn.getresponse()
            json_data: bytes = res.read()

            return json.loads(json_data.decode("utf-8"))

        except Exception:
            logger.exception("Error while getting data")
            return {}
