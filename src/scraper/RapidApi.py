import requests
import time
from tqdm import tqdm
import math


class RapidApiScraper:
    def __init__(self, api_key: str):
        self.api_key = api_key

    def get_users_tweets_by_twitter_api45(
        self, username: str, expected_num_tweets: int = None
    ) -> list[dict]:
        """https://rapidapi.com/alexanderxbx/api/twitter-api45"""
        url = "https://twitter-api45.p.rapidapi.com/search.php"
        querystring = {"query": f"(from:{username})", "search_type": "Latest"}
        headers = {
            "x-rapidapi-key": self.api_key,
            "x-rapidapi-host": "twitter-api45.p.rapidapi.com",
        }
        tweets = []
        retry_count = 0
        max_retries = 5
        have_data = True
        status500_retry_seconds = 30
        TWEETS_PER_REQUEST = 20
        pbar = tqdm(
            desc=f"Fetching tweets for {username}",
            unit="tweets",
            leave=False,
            total=math.ceil(expected_num_tweets / TWEETS_PER_REQUEST)
            if expected_num_tweets
            else None,
        )
        try:
            while have_data:
                response = requests.get(url, headers=headers, params=querystring)

                if response.status_code == 500:
                    print(f"Looks like the request failed for user {username}.")
                    print(f"Status {response.status_code}: {response.text}")
                    print(f"Retrying in {status500_retry_seconds} seconds...")
                    time.sleep(status500_retry_seconds)
                    continue

                if response.status_code != 200:
                    print(
                        f"Error fetching data for user {username}: {response.status_code}"
                    )
                    print(f"Response: {response.text}")
                    break

                data = response.json()

                if len(data["timeline"]) == 0:
                    time.sleep(30)
                    retry_count += 1
                    print(
                        f"No data for user {username}. Retrying {retry_count}/{max_retries}..."
                    )
                    if retry_count > max_retries:
                        print(
                            f"No more data for user {username}. Stopped after {max_retries} retries."
                        )
                        break
                    continue

                retry_count = 0
                tweets.extend(data["timeline"])
                pbar.update(1)

                if not data["next_cursor"]:
                    have_data = False
                querystring["cursor"] = data.get("next_cursor", None)
        except Exception as e:
            print(f"An error occurred: {e}")

        return tweets
