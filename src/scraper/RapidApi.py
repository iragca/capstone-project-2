import requests
import time
from tqdm import tqdm
import math


class RapidApiScraper:
    def __init__(self, api_key: str):
        self.api_key = api_key

    def get_users_tweets_by_twitter_api45(
        self, username: str, expected_num_tweets: int = None, max_retries: int = 5
    ) -> list[dict]:
        """https://rapidapi.com/alexanderxbx/api/twitter-api45"""
        url = "https://twitter-api45.p.rapidapi.com/search.php"
        querystring = {
            "query": f"(from:{username}) until:2020-07-24",
            "search_type": "Latest",
        }
        headers = {
            "x-rapidapi-key": self.api_key,
            "x-rapidapi-host": "twitter-api45.p.rapidapi.com",
        }
        tweets = []
        retry_count = 0
        have_data = True
        status500_retry_seconds = 30
        TWEETS_PER_PAGE = 20
        print(f"{status500_retry_seconds=}, {TWEETS_PER_PAGE=}")

        pbar = tqdm(
            desc=f"Fetching tweet pages for {username}",
            unit="pages",
            leave=False,
            total=math.ceil(expected_num_tweets / TWEETS_PER_PAGE)
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

                timeline_length = len(data["timeline"])
                if (timeline_length == 0) and (data["status"] != "ok"):
                    time.sleep(30)
                    retry_count += 1
                    print(
                        f"No additional tweets found for user {username}. Retrying {retry_count}/{max_retries}..."
                    )
                    print(f"Response: {data}")
                    if retry_count > max_retries:
                        print(
                            f"No more data for user {username}. Stopped after {max_retries} retries."
                        )
                        break
                    continue
                else:
                    retry_count = 0

                if data["status"] == "ok" and timeline_length == 0:
                    print(f"No more tweets found for user {username}. Stopped.")
                    break

                tweets.extend(data["timeline"])
                pbar.update(1)

                if not data["next_cursor"]:
                    have_data = False
                    pbar.close()
                    break
                querystring["cursor"] = data.get("next_cursor", None)
        except Exception as e:
            print(f"An error occurred: {e}")

        return tweets
