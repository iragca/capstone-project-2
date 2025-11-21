from typing import Literal

import networkx as nx
import torch

from src.models import TweetLinkProbability


class InferenceResults:
    """
    Encapsulates the results of a node embedding-based inference process on a graph.

    This class provides methods to access node and edge information, retrieve nodes by index
    or label, and compute similarity-based predictions between nodes.

    Parameters
    ----------
    graph : nx.Graph
        The input graph, typically a NetworkX Graph object.
    node_embeddings : torch.Tensor
        Node embedding matrix of shape (num_nodes, embedding_dim).
    edge_label_index : torch.Tensor
        Tensor of shape [2, num_edges] indicating edge indices.

    Attributes
    ----------
    graph : nx.Graph
        The underlying graph.
    node_embeddings : torch.Tensor
        Embeddings for each node.
    edge_label_index : torch.Tensor
        Edge indices for label prediction.
    ...

    Raises
    ------
    TypeError
        If input types are incorrect.
    ValueError
        If input shapes or graph/embedding/edge counts are inconsistent.
    """

    def __init__(
        self,
        graph: nx.Graph,
        node_embeddings: torch.Tensor,
        edge_label_index: torch.Tensor,
    ):
        self.graph = graph
        self.node_embeddings = node_embeddings
        self.edge_label_index = edge_label_index
        self.node_list = list(graph.nodes(data=True))
        self.node_map = {index: node for index, node in enumerate(self.node_list)}
        self.nodes_first = torch.index_select(
            node_embeddings, 0, edge_label_index[0, :].long()
        )
        self.nodes_second = torch.index_select(
            node_embeddings, 0, edge_label_index[1, :].long()
        )
        self.logits = torch.sum(self.nodes_first * self.nodes_second, dim=-1)
        self.predictions = torch.sigmoid(self.logits)

        if not isinstance(graph, nx.Graph):
            raise TypeError("Graph must be an instance of networkx.Graph.")

        if not isinstance(node_embeddings, torch.Tensor):
            raise TypeError("Node embeddings must be a torch.Tensor.")

        if not isinstance(edge_label_index, torch.Tensor):
            raise TypeError("Edge label index must be a torch.Tensor.")

        if graph.number_of_nodes() != node_embeddings.shape[0]:
            raise ValueError("Graph and embeddings must have the same number of nodes.")

        if edge_label_index.ndim != 2:
            raise ValueError("Edge label index must have shape [2, num_edges].")

    def get_node_using_node_index(self, node_index: int) -> tuple[int, dict]:
        """
        Get the node using its index in the graph.

        Parameters
        ----------
        node_index : int
            The index of the node.

        Returns
        -------
        tuple of (int, dict)
            The node ID and its attributes.

        Examples
        --------
        >>> inferenceresults.get_node_using_node_index(0)
        (1277976913743503365,
            {'node_label': 1,
            'node_feature': tensor([4.8600e+03, 3.3400e+02, 2.8000e+01, 0.0000e+00]),
            'node_type': 'tweet'})
        """

        return self.node_list[node_index]

    def get_node_index_using_node(self, node_id: int) -> int:
        """
        Get the index of a node in the graph by its ID.

        Parameters
        ----------
        node_id : int
            The ID of the node to search for in the graph.

        Returns
        -------
        int
            The index of the node in `self.node_list`.

        Raises
        ------
        ValueError
            If the node with the given ID is not found.
        """

        for index, (node, _) in enumerate(self.node_list):
            if node == node_id:
                return index

    def get_index_of_nodes_with_label(self, label: Literal[0, 1, 2, 3]) -> list[int]:
        """
        Get the indices of nodes with a specific label.

        Node labels are defined as:
            0 : hateful / extremist tweet
            1 : non-hateful / non-extremist / neutral tweet
            2 : offensive tweet
            3 : user

        Parameters
        ----------
        label : int
            The label to filter nodes by. Must be one of [0, 1, 2, 3].

        Returns
        -------
        list[int]
            A list of indices corresponding to nodes with the specified label.
        """

        return [
            index
            for index, node in enumerate(self.node_list)
            if node[1]["node_label"] == label
        ]

    def get_top_k_similar_nodes_linked_to_user(
        self,
        user_id: int,
        k: int = 10,
        descending: bool = True,
        label: Literal[0, 1, 2, 3] = 0,
    ) -> list[TweetLinkProbability]:
        """
        Get the top-k nodes of a certain label most likely connected to a given user.

        Parameters
        ----------
        user_id : int
            The ID of the user node.
        k : int, optional
            Number of top similar nodes to return (default is 10).
        descending : bool, optional
            If True, sort probabilities from high to low (default is True).
        label : int, optional
            Label of candidate nodes to consider (default is 0). Must be one of [0, 1, 2, 3].

        Returns
        -------
        list of TweetLinkProbability
            A list of `TweetLinkProbability` objects, sorted by score/probability.

        Raises
        ------
        TypeError
            If `user_id` is not an integer.
        ValueError
            If `k` is not a positive integer, `label` is invalid, or the user is not found.
        """

        if not isinstance(user_id, int):
            raise TypeError("user_id must be an integer.")

        if not isinstance(k, int) or k <= 0:
            raise ValueError("k must be a positive integer.")

        if label not in [0, 1, 2, 3]:
            raise ValueError("Label must be one of [0, 1, 2, 3].")

        user_index = self.get_node_index_using_node(user_id)
        if user_index is None:
            raise ValueError(f"User with ID {user_id} not found in the graph.")

        candidate_indexes = self.get_index_of_nodes_with_label(label)
        if not candidate_indexes:
            return []

        candidate_embeddings = self.node_embeddings[
            candidate_indexes
        ]  # (num_candidates, dim)
        user_embedding = self.node_embeddings[user_index]  # (dim,)

        # Compute dot product similarity and convert to probability
        logits = torch.matmul(candidate_embeddings, user_embedding)  # (num_candidates,)
        probs = torch.sigmoid(logits)  # (num_candidates,)

        # Get node IDs from indices
        candidate_node_ids = [self.node_list[i][0] for i in candidate_indexes]

        # Pair each node ID with its probability
        scored = list(zip(candidate_node_ids, probs.tolist()))

        # Sort and return top-k
        scored_sorted = sorted(scored, key=lambda x: x[1], reverse=descending)
        return scored_sorted[:k]
