from typing import Literal

import polars as pl
import torch

from src.models import DiGraph, Features, Graph


class GraphBuilder:
    """
    Construct a bipartite (optionally heterogeneous and/or directed) graph from a tabular dataset.

    The graph contains nodes for tweets and users with assigned features and labels, and edges
    connecting users to tweets. Optionally supports heterogeneous graphs with edge types.

    Parameters
    ----------
    data : pl.DataFrame
        The input dataset containing tweet and user information.
    node_features : Features
        An object specifying which features to extract for tweet and user nodes.

    Attributes
    ----------
    data : pl.DataFrame
        The dataset used to build the graph.
    node_features : Features
        The features specification for nodes.
    max_features : int
        The maximum number of features among tweet and user nodes, used for padding.

    Methods
    -------
    create_graph(directed=False, heterogeneous=False) -> Graph | DiGraph
        Constructs and returns a bipartite graph with nodes and edges.
    get_features(row, node_type) -> torch.Tensor
        Extract features for a tweet or user node as a padded tensor.
    _validate_graph_inputs(data) -> None
        Validate the input DataFrame for type, emptiness, and nulls.
    _check_no_nulls(data) -> None
        Raise an error if the DataFrame contains null values.
    _check_is_dataframe(data) -> None
        Raise an error if the input is not a polars DataFrame.
    _check_not_empty(data) -> None
        Raise an error if the DataFrame is empty.
    _has_null(data) -> bool
        Check if the DataFrame contains any null values.
    """

    def __init__(self, data: pl.DataFrame, node_features: Features):
        self.data = data
        self.node_features = node_features
        self.max_features = max(len(node_features.tweet), len(node_features.user))

    def create_graph(
        self, directed: bool = False, heterogeneous: bool = False
    ) -> Graph | DiGraph:
        """
        Create a bipartite graph from the dataset.

        Parameters
        ----------
        directed : bool, optional
            Whether to create a directed graph (default is False).
        heterogeneous : bool, optional
            Whether to create a heterogeneous graph with different edge types (default is False).

        Returns
        -------
        Graph | DiGraph
            A bipartite graph with tweet and user nodes, optionally directed or heterogeneous.

        Raises
        ------
        ValueError
            If tweet and user features have mismatched shapes.
        """

        self._validate_graph_inputs(self.data)

        if directed:
            G = DiGraph()
        else:
            G = Graph()

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
        """
        Extract features for a node from a dataset row.

        Parameters
        ----------
        row : dict
            A dictionary representing a single row from the dataset.
        node_type : {"tweet", "user"}
            The type of node to extract features for.

        Returns
        -------
        torch.Tensor
            A tensor of node features, padded to `max_features` if necessary.

        Raises
        ------
        ValueError
            If `node_type` is not "tweet" or "user".
        """

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
        """
        Validate input DataFrame for graph creation.

        Checks type, non-emptiness, and absence of null values.

        Parameters
        ----------
        data : pl.DataFrame
            Dataset to validate.

        Raises
        ------
        TypeError
            If `data` is not a polars DataFrame.
        ValueError
            If `data` is empty or contains null values.
        """
        self._check_is_dataframe(data)
        self._check_not_empty(data)
        self._check_no_nulls(data)

    def _check_no_nulls(self, data: pl.DataFrame) -> None:
        """
        Raise an error if the dataset contains null values.

        Parameters
        ----------
        data : pl.DataFrame
            Dataset to check.

        Raises
        ------
        ValueError
            If any null values are found.
        """
        if self._has_null(data):
            raise ValueError("Dataset contains null values. Please clean the data.")

    @staticmethod
    def _check_is_dataframe(data: pl.DataFrame) -> None:
        """
        Ensure the input is a polars DataFrame.

        Parameters
        ----------
        data : pl.DataFrame
            Dataset to check.

        Raises
        ------
        TypeError
            If `data` is not a polars DataFrame.
        """
        if not isinstance(data, pl.DataFrame):
            raise TypeError("Data must be a polars DataFrame.")

    @staticmethod
    def _check_not_empty(data: pl.DataFrame) -> None:
        """
        Ensure the dataset is not empty.

        Parameters
        ----------
        data : pl.DataFrame
            Dataset to check.

        Raises
        ------
        ValueError
            If the dataset is empty.
        """
        if data.is_empty():
            raise ValueError("Dataset is empty. Please load a valid dataset.")

    @staticmethod
    def _has_null(data: pl.DataFrame) -> bool:
        """
        Check if the dataset contains any null values.

        Parameters
        ----------
        data : pl.DataFrame
            Dataset to check.

        Returns
        -------
        bool
            True if any null values exist, False otherwise.
        """
        return data.null_count().to_numpy().sum() > 0
