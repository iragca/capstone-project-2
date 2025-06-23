import json
from pathlib import Path

import requests
from tqdm import tqdm

from src.config import logger


def get_tweet_replies(tweet_ids: list[str], staging: Path) -> None:
    assert isinstance(tweet_ids, list), "tweet_ids must be a list of strings"
    assert all(
        isinstance(tweet_id, str) for tweet_id in tweet_ids
    ), "All tweet_ids must be strings"
    url = "https://twitter154.p.rapidapi.com/tweet/replies/continuation"
    headers = {
        "x-rapidapi-key": "3ba6bea96amsha13f50dd29c930fp1f1cf9jsnc15627770e18",
        "x-rapidapi-host": "twitter154.p.rapidapi.com",
    }

    for tweet_id in tqdm(tweet_ids, desc="Fetching tweets", unit="tweet"):

        querystring = {
            "tweet_id": tweet_id,
            "continuation_token": "ZAAAAPBVHBmm-Iawof7U97U19Me0obLCjbc19oe2-Z3e8rU1xIXYkai08rU1wIa7vd2j_7Y1kILTkeeGo7Y1-IXYqdGO0uM0qIfY6YSE7rU16oLY-aqw9LU1sMe8id6277U1JQISFQQAAA",
        }

        response = requests.get(url, headers=headers, params=querystring)
        data = response.json()

  

        if response.status_code != 200:
            logger.error(
                f"Error fetching data: {response.status_code} - {data.get('message', 'No message')}"
            )
            break

        if "replies" not in data:
            logger.error("No replies found in the response")
            break

        replies = data["replies"]

        if len(replies) == 0:
            logger.warning(f"No replies found for tweet_id {tweet_id} in this request.")

        for tweet in replies:
            json_filename = staging / f"{tweet['tweet_id']}.json"
            with open(json_filename, "w", encoding="utf-8") as f:
                json.dump(tweet, f, ensure_ascii=False, indent=4)
