from pprint import pprint

from ...models import Tweet
from . import blm_hashtag


def api45_tweet(tweet_data: dict) -> Tweet:
    """
    Parses a tweet data dictionary into a Tweet model instance.

    Args:
        tweet_data (dict): The tweet data dictionary.

    Returns:
        Tweet: An instance of the Tweet model.
    """
    assert isinstance(tweet_data, dict), "Input must be a dictionary"

    try:
        tweet_id = tweet_data.get("tweet_id")
        text = tweet_data.get("text")
        in_reply_to_status_id = tweet_data.get("in_reply_to_status_id_str", None)
        bookmark_count = tweet_data.get("bookmarks", 0)
        views = tweet_data.get("views", 0)
        retweet_count = tweet_data.get("retweets", 0)
        favorite_count = tweet_data.get("favorites", 0)
        reply_count = tweet_data.get("replies", 0)
        quote_count = tweet_data.get("quotes", 0)
        conversation_id = tweet_data.get("conversation_id")
        language = tweet_data.get("lang", "")
        source = tweet_data.get("source", "")
        creation_date = tweet_data.get("created_at", "")
        has_blm_hashtag: bool = blm_hashtag(text)
        retweet_status_id = tweet_data.get("retweeted", {}).get("tweet_id", None)
        quoted_status_id = tweet_data.get("quoted", {}).get("tweet_id", None)
        community_note = tweet_data.get("community_note", "")

        user_id = tweet_data.get("user_info", {}).get("rest_id")
        username = tweet_data.get("user_info", {}).get("screen_name")

        if conversation_id is None:
            conversation_id = tweet_id

        if not user_id:
            # This happens when the tweet is a quote
            user_id = tweet_data.get("author").get("rest_id")
            username = tweet_data.get("author").get("screen_name")

        status_link = f"https://x.com/{username}/status/{tweet_id}"
    except Exception as e:
        pprint(f"Error processing tweet data: {e}, {tweet_data}")

    return Tweet(
        tweet_id=tweet_id,
        text=text,
        status_link=status_link,
        user_id=user_id,
        in_reply_to_status_id=in_reply_to_status_id,
        bookmark_count=bookmark_count,
        views=views,
        retweet_count=retweet_count,
        favorite_count=favorite_count,
        reply_count=reply_count,
        quote_count=quote_count,
        conversation_id=conversation_id,
        language=language,
        source=source,
        creation_date=creation_date,
        has_blm_hashtag=has_blm_hashtag,
        retweet_status_id=retweet_status_id,
        quoted_status_id=quoted_status_id,
        community_note=community_note,
    )
