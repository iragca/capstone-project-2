import networkx as nx
import polars as pl
import torch

from src.config import PROCESSED_DATA_DIR


def load_data() -> pl.DataFrame:
    data = pl.read_csv(
        PROCESSED_DATA_DIR / "hatebert-test_with_users.csv",
        columns=[
            "tweet_id",
            # "text",
            # "status_link",
            # "is_extremist",
            # "is_annotated",
            # "in_reply_to_status_link",
            # "in_reply_to_status_id",
            "bookmark_count",
            "views",
            "retweet_count",
            "favorite_count",
            "reply_count",
            "quote_count",
            # "conversation_id",
            # "retweet_tweet_id",
            # "quoted_status_id",
            # "community_note",
            # "language",
            # "source",
            # "has_blm_hashtag",
            # "fetched_replies",
            # "is_reply_to_blm",
            "is_hateful",
            # "id",
            # "created",
            # "updated",
            # "bot",
            # "collection_id",
            # "collection_name",
            # "creation_date",
            # "description",
            "favourites_count",
            "follower_count",
            "following_count",
            "is_blue_verified",
            # "is_private",
            # "is_verified",
            "listed_count",
            # "location",
            # "name",
            "number_of_tweets",
            "user_id",
            # "username",
            # "tweet_creation_date",
        ],
        schema={
            "tweet_id": pl.Utf8,
            "text": pl.Utf8,
            "status_link": pl.Utf8,
            "is_extremist": pl.Boolean,
            "is_annotated": pl.Boolean,
            "in_reply_to_status_link": pl.Utf8,
            "in_reply_to_status_id": pl.Utf8,
            "bookmark_count": pl.Int64,
            "views": pl.Int64,
            "retweet_count": pl.Int64,
            "favorite_count": pl.Int64,
            "reply_count": pl.Int64,
            "quote_count": pl.Int64,
            "conversation_id": pl.Utf8,
            "retweet_tweet_id": pl.Utf8,
            "quoted_status_id": pl.Utf8,
            "community_note": pl.Utf8,
            "language": pl.Utf8,
            "source": pl.Utf8,
            "has_blm_hashtag": pl.Boolean,
            "fetched_replies": pl.Boolean,
            "is_reply_to_blm": pl.Boolean,
            "is_hateful": pl.Int8,
            "id": pl.Utf8,
            "created": pl.Utf8,
            "updated": pl.Utf8,
            "bot": pl.Boolean,
            "collection_id": pl.Utf8,
            "collection_name": pl.Utf8,
            "creation_date": pl.Utf8,
            "description": pl.Utf8,
            "favourites_count": pl.Int64,
            "follower_count": pl.Int64,
            "following_count": pl.Int64,
            "is_blue_verified": pl.Boolean,
            "is_private": pl.Boolean,
            "is_verified": pl.Boolean,
            "listed_count": pl.Int64,
            "location": pl.Utf8,
            "name": pl.Utf8,
            "number_of_tweets": pl.Int64,
            "user_id": pl.Utf8,
            "username": pl.Utf8,
            "tweet_creation_date": pl.Utf8,
        },
    )

    return data


def create_graph(data: pl.DataFrame) -> nx.Graph:
    G = nx.DiGraph()

    for row in data.iter_rows(named=True):
        tweet_id = row["tweet_id"]
        is_hateful = row["is_hateful"]
        user_id = row["user_id"]

        tweet_features = torch.tensor(
            [
                row["favorite_count"],
                row["retweet_count"],
                row["bookmark_count"],
                row["reply_count"],
                row["quote_count"],
                row["views"],
                row["is_hateful"],
            ],
            dtype=torch.float32,
        )

        user_features = torch.tensor(
            [
                row["favourites_count"],
                row["follower_count"],
                row["following_count"],
                row["number_of_tweets"],
                row["listed_count"],
                row["is_blue_verified"],
                0,
            ],
            dtype=torch.float32,
        )

        G.add_node(
            tweet_id, node_label=is_hateful, bipartite=0, node_feature=tweet_features
        )
        G.add_node(user_id, node_label=3, bipartite=1, node_feature=user_features)
        if tweet_id and user_id:
            G.add_edge(tweet_id, user_id)

    return G
