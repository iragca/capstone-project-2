import random
import networkx as nx


class UserGraphMixin:
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

    def has_node(self, node_id: int) -> bool:
        """
        Check if a node with the given ID exists in the graph.

        Args:
            node_id (int): The ID of the node to check.

        Returns:
            bool: True if the node exists, False otherwise.
        """
        return node_id in self.nodes()


class Graph(UserGraphMixin, nx.Graph):
    pass


class DiGraph(UserGraphMixin, nx.DiGraph):
    pass
