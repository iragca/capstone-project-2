from typing import Literal

import networkx as nx
import polars as pl
import torch


class GraphBuilder:
    def __init__(self, data: pl.DataFrame):
        self.data = data

    def create_graph(self, dtype: nx.Graph | nx.DiGraph) -> nx.Graph | nx.DiGraph:
        """Create a bipartite graph from the dataset.

        Args:
            data (pl.DataFrame): The dataset to create the graph from.
            dtype (nx.Graph | nx.DiGraph): The type of graph to create.

        Returns:
            nx.Graph | nx.DiGraph: The created graph.
        """

        self._validate_graph_inputs(self.data, dtype)

        G: nx.Graph | nx.DiGraph = dtype()

        for row in self.data.iter_rows(named=True):
            tweet_id = row["tweet_id"]
            is_hateful = row["is_hateful"]
            user_id = row["user_id"]

            tweet_features = self.get_features(row, "tweet")
            user_features = self.get_features(row, "user")

            if tweet_features.shape != user_features.shape:
                raise ValueError("Tweet and user features must have the same shape.")

            G.add_node(
                tweet_id,
                node_label=is_hateful,
                bipartite=0,
                node_feature=tweet_features,
            )
            G.add_node(user_id, node_label=3, bipartite=1, node_feature=user_features)
            if tweet_id and user_id:
                G.add_edge(tweet_id, user_id)

        return G

    def get_features(
        self, row: dict, node_type: Literal["tweet", "user"]
    ) -> torch.Tensor:
        """Get features for a tweet node."""
        match node_type:
            case "user":
                return torch.tensor(
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

            case "tweet":
                return torch.tensor(
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
            case _:
                raise ValueError("node_type must be either 'tweet' or 'user'.")

    def _validate_graph_inputs(self, data: pl.DataFrame, dtype: type) -> None:
        self._check_is_dataframe(data)
        self._check_not_empty(data)
        self._check_graph_type(dtype)
        self._check_no_nulls(data)

    def _check_no_nulls(self, data: pl.DataFrame) -> None:
        if self._has_null(data):
            raise ValueError("Dataset contains null values. Please clean the data.")

    @staticmethod
    def _check_is_dataframe(data: pl.DataFrame) -> None:
        if not isinstance(data, pl.DataFrame):
            raise TypeError("Data must be a polars DataFrame.")

    @staticmethod
    def _check_not_empty(data: pl.DataFrame) -> None:
        if data.is_empty():
            raise ValueError("Dataset is empty. Please load a valid dataset.")

    @staticmethod
    def _check_graph_type(dtype: type) -> None:
        if dtype not in (nx.Graph, nx.DiGraph):
            raise ValueError("Unsupported graph type. Use nx.Graph or nx.DiGraph.")

    @staticmethod
    def _has_null(data: pl.DataFrame) -> bool:
        """Check if the DataFrame has any null values."""
        return data.null_count().to_numpy().sum() > 0
