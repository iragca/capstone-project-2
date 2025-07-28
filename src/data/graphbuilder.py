import random
from typing import Literal

import networkx as nx
import polars as pl
import torch

from src.models import Features


class CustomGraph(nx.Graph):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def get_random_user(self) -> int:
        """
        Get a random user node from the graph.

        Returns:
            int: The ID of a random user node.
        """
        user_nodes = [
            node
            for node, data in self.nodes(data=True)
            if data.get("node_type") == "user"
        ]
        if not user_nodes:
            raise ValueError("No user nodes found in the graph.")
        return random.choice(user_nodes)


class GraphBuilder:
    def __init__(self, data: pl.DataFrame, node_features: Features):
        self.data = data
        self.node_features = node_features
        self.max_features = max(len(node_features.tweet), len(node_features.user))

    def create_graph(
        self, directed: bool = False, heterogeneous: bool = False
    ) -> nx.Graph | nx.DiGraph:
        """Create a bipartite graph from the dataset.

        Args:
            directed (bool): Whether to create a directed graph. Defaults to False.
            heterogeneous (bool): Whether to create a heterogeneous graph with different edge types. Defaults to False.

        Returns:
            nx.Graph | nx.DiGraph: The created graph.
        """

        self._validate_graph_inputs(self.data)

        if directed:
            G = nx.DiGraph()
        else:
            G = CustomGraph()

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
                node_feature=tweet_features,
                node_type="n0" if heterogeneous else "tweet",
            )
            G.add_node(
                user_id,
                node_label=3,
                node_feature=user_features,
                node_type="n0" if heterogeneous else "user",
            )
            if tweet_id and user_id:
                if heterogeneous:
                    # `G.add_edge(user_id -> tweet_id)` if directed
                    G.add_edge(
                        user_id,
                        tweet_id,
                        edge_feature=torch.zeros(
                            self.max_features, dtype=torch.float32
                        ),
                        edge_type="replied_to",
                    )
                else:
                    # `G.add_edge(user_id -> tweet_id)` if directed
                    G.add_edge(user_id, tweet_id)

        return G

    def get_features(
        self, row: dict, node_type: Literal["tweet", "user"]
    ) -> torch.Tensor:
        """Get features for a tweet node."""
        if node_type not in ["tweet", "user"]:
            raise ValueError("Invalid node type. Use 'tweet' or 'user'.")

        model_dict: dict = self.node_features.model_dump()
        feature_list: list[str] = model_dict[node_type]

        features_dict = {feature: row[feature] for feature in feature_list}

        features_list = list(features_dict.values())

        if len(features_list) < self.max_features:
            # Pad the features list with zeros if it is shorter than max_features
            features_list += [0] * (self.max_features - len(features_list))

        return torch.tensor(features_list, dtype=torch.float32)

    def _validate_graph_inputs(self, data: pl.DataFrame) -> None:
        self._check_is_dataframe(data)
        self._check_not_empty(data)
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
    def _has_null(data: pl.DataFrame) -> bool:
        """Check if the DataFrame has any null values."""
        return data.null_count().to_numpy().sum() > 0
