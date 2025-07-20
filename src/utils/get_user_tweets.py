import requests
from tqdm import tqdm


def get_user_tweets(user_id: str, username: str, cont_token: str,  api_key: str, max_requests: int = 100) -> list:
    url = "https://twitter154.p.rapidapi.com/user/tweets/continuation"
    headers = {
        "x-rapidapi-key": api_key,
        "x-rapidapi-host": "twitter154.p.rapidapi.com",
    }
    tweets = []
    querystring = {
        "limit": "20",
        "continuation_token": cont_token,
        "user_id": user_id,
        "username": username,
        "include_replies": "false",
    }

    try:
        for _ in tqdm(range(max_requests), desc=f"Fetching tweets for user {user_id}"):
            if not cont_token:
                break

            response = requests.get(url, headers=headers, params=querystring)
            if response.status_code != 200:
                raise Exception(
                    f"Error fetching tweets: {response.status_code} - {response.text}"
                )
            

            data = response.json()
            results = data.get("results", [])

            if not results:
                print(f"No more tweets found for user {user_id}.")
                break

            if len(results) == 0:
                print(f"No tweets found for user {user_id}.")
                break

            tweets.extend(data.get("results", []))
            cont_token = data.get("continuation_token", "")

            if cont_token == "":
                querystring["continuation_token"] = cont_token
    except Exception as e:
        raise Exception(f"Request failed: {e}")

    return tweets
