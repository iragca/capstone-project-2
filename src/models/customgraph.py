import random

import networkx as nx


class UserGraphMixin:
    """
    Mixin class providing utility methods for graphs with user nodes.

    This class is intended to be used with NetworkX graphs where nodes may
    have an attribute ``node_type``. It provides helper methods for working
    specifically with nodes labeled as users.
    """

    def get_random_user(self) -> int:
        """
        Get a random user node from the graph.

        Returns
        -------
        int
            The ID of a randomly selected user node.

        Raises
        ------
        ValueError
            If no nodes with ``node_type='user'`` exist in the graph.

        Examples
        --------
        >>> import networkx as nx
        >>> from mypackage.graphs import Graph
        >>> G = Graph()
        >>> G.add_node(1, node_type="user")
        >>> G.add_node(2, node_type="tweet")
        >>> G.get_random_user()
        1
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

        Parameters
        ----------
        node_id : int
            The ID of the node to check.

        Returns
        -------
        bool
            ``True`` if the node exists, ``False`` otherwise.

        Examples
        --------
        >>> G = Graph()
        >>> G.add_node(1, node_type="user")
        >>> G.has_node(1)
        True
        >>> G.has_node(2)
        False
        """
        return node_id in self.nodes()


class Graph(UserGraphMixin, nx.Graph):
    """
    Undirected graph with user-specific utility methods.

    Inherits from both :class:`networkx.Graph` and :class:`UserGraphMixin`.

    Examples
    --------
    >>> G = Graph()
    >>> G.add_node(1, node_type="user")
    >>> G.add_node(2, node_type="tweet")
    >>> G.add_edge(1, 2)
    >>> G.get_random_user()
    1
    """

    pass


class DiGraph(UserGraphMixin, nx.DiGraph):
    """
    Directed graph with user-specific utility methods.

    Inherits from both :class:`networkx.DiGraph` and :class:`UserGraphMixin`.

    Examples
    --------
    >>> G = DiGraph()
    >>> G.add_node(1, node_type="user")
    >>> G.add_node(2, node_type="tweet")
    >>> G.add_edge(1, 2)
    >>> G.has_node(1)
    True
    """

    pass
