from typing import Literal

import networkx as nx
import torch


class InferenceResults:
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
        self.nodes_first = torch.index_select(node_embeddings, 0, edge_label_index[0, :].long())
        self.nodes_second = torch.index_select(node_embeddings, 0, edge_label_index[1, :].long())
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

        if graph.number_of_edges() != edge_label_index.shape[1] / 2:
            raise ValueError(
                "Graph and edge label index must have the same number of edges."
            )

    def get_node_using_node_index(self, node_index: int):
        """
        Get the node using its index in the graph.
        """
        return self.node_list[node_index]

    def get_node_index_using_node(self, node_id: int) -> int:
        """
        Get the index of a node in the graph by its ID.

        Args:
            graph (Graph): The graph object.
            node_id (int): The ID of the node.

        Returns:
            int: The index of the node in the graph.
        """
        for index, (node, _) in enumerate(self.node_list):
            if node == node_id:
                return index

    def get_index_of_nodes_with_label(self, label: Literal[0, 1, 2, 3]) -> list[int]:
        """
        Get the indices of nodes with a specific label.
        labels:
            0: hateful / extremist tweet
            1: non-hateful / non-extremist / neutral tweet
            2: offensive tweet
            3: user

        Args:
            label (int): The label to filter nodes by.

        Returns:
            list[int]: A list of indices of nodes with the specified label.
        """

        return [
            index
            for index, node in enumerate(self.node_list)
            if node[1]["node_label"] == label
        ]

    def get_top_k_similar_nodes_linked_to_user(
        self, user_id: int, k: int = 10, descending: bool = True, label: Literal[0, 1, 2, 3] = 0
    ) -> list[int]:
        """
        Get the top k similar nodes linked to a specific user.

        Args:
            user_id (int): The ID of the user node.
            k (int): The number of top similar nodes to return.

        Returns:
            list[int]: A list of indices of the top k similar nodes linked to the user.
        """
        user_id_index = self.get_node_index_using_node(user_id)

        if user_id_index is None:
            raise ValueError(f"User with ID {user_id} not found in the graph.")

        similar_nodes_indexes = self.get_index_of_nodes_with_label(label)
        similar_nodes_ids = [self.node_list[index][0] for index in similar_nodes_indexes]

        node_logits = torch.sum(self.nodes_second * torch.index_select(self.node_embeddings, 0, torch.tensor([user_id_index])), dim=-1)
        node_pred = torch.sigmoid(node_logits)

        scores = []
        for thing in zip(similar_nodes_ids, torch.index_select(node_pred, 0, torch.tensor(similar_nodes_indexes))):
            scores.append(thing)

        scores = sorted(scores, key=lambda x: x[1], reverse=descending)

        return scores[:k]

