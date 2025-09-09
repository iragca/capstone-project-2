import networkx as nx
import torch
import torch.nn as nn
from deepsnap.hetero_gnn import HeteroConv, HeteroSAGEConv, forward_op


class HeteroGNN(torch.nn.Module):
    """
    Heterogeneous Graph Neural Network (GNN) for link prediction.

    This model applies two layers of heterogeneous message passing (via
    :class:`deepsnap.hetero_gnn.HeteroConv`) with batch normalization and
    non-linear activation. Predictions are computed as the dot product
    between pairs of node embeddings.

    Parameters
    ----------
    hetero : networkx.Graph
        A heterogeneous graph object compatible with DeepSNAP, containing
        node and edge type metadata.
    hidden_size : int
        Dimensionality of hidden representations for all node types.

    Attributes
    ----------
    convs1 : HeteroConv
        First heterogeneous convolutional layer.
    convs2 : HeteroConv
        Second heterogeneous convolutional layer.
    bns1 : nn.ModuleDict
        Batch normalization modules for the first layer, per node type.
    bns2 : nn.ModuleDict
        Batch normalization modules for the second layer, per node type.
    relus1 : nn.ModuleDict
        Activation functions (LeakyReLU) after the first layer, per node type.
    relus2 : nn.ModuleDict
        Activation functions (LeakyReLU) after the second layer, per node type.
    loss_fn : torch.nn.BCEWithLogitsLoss
        Binary cross-entropy loss with logits.

    Examples
    --------
    >>> from deepsnap.dataset import GraphDataset
    >>> dataset = GraphDataset("hetero-example")
    >>> data = dataset[0]
    >>> model = HeteroGNN(data.G, hidden_size=64)
    >>> pred = model(data)
    >>> loss = model.loss(pred, data.edge_label)
    """

    def __init__(self, hetero, hidden_size):
        super(HeteroGNN, self).__init__()

        conv1, conv2 = self.generate_2convs_link_pred_layers(
            hete=hetero,
            conv=HeteroSAGEConv,
            hidden_size=hidden_size,
        )

        self.convs1 = HeteroConv(conv1)
        self.convs2 = HeteroConv(conv2)
        self.loss_fn = torch.nn.BCEWithLogitsLoss()
        self.bns1 = nn.ModuleDict()
        self.bns2 = nn.ModuleDict()
        self.relus1 = nn.ModuleDict()
        self.relus2 = nn.ModuleDict()
        self.post_mps = nn.ModuleDict()

        for node_type in hetero.node_types:
            self.bns1[node_type] = torch.nn.BatchNorm1d(hidden_size)
            self.bns2[node_type] = torch.nn.BatchNorm1d(hidden_size)
            self.relus1[node_type] = nn.LeakyReLU()
            self.relus2[node_type] = nn.LeakyReLU()

    def forward(self, data):
        """
        Forward pass of the heterogeneous GNN.

        Parameters
        ----------
        data : object
            DeepSNAP heterogeneous graph data object with attributes:
            - ``node_feature`` : dict of {node_type: torch.Tensor}
              Node feature matrices.
            - ``edge_index`` : dict of {message_type: torch.LongTensor}
              Edge indices in COO format, keyed by message type.
            - ``edge_label_index`` : dict of {message_type: torch.LongTensor}
              Candidate edge pairs for link prediction.

        Returns
        -------
        dict
            Dictionary of predictions per message type:
            - key: message type tuple (src, relation, dst)
            - value: torch.Tensor of shape (num_edges,)
              Predicted logits for edges of this type.
        """
        x = data.node_feature
        edge_index = data.edge_index
        x = self.convs1(x, edge_index)
        x = forward_op(x, self.bns1)
        x = forward_op(x, self.relus1)
        x = self.convs2(x, edge_index)
        x = forward_op(x, self.bns2)

        pred = {}
        for message_type in data.edge_label_index:
            nodes_first = torch.index_select(
                x["n1"], 0, data.edge_label_index[message_type][0, :].long()
            )
            nodes_second = torch.index_select(
                x["n1"], 0, data.edge_label_index[message_type][1, :].long()
            )
            pred[message_type] = torch.sum(nodes_first * nodes_second, dim=-1)
        return pred

    def loss(self, pred, y):
        """
        Compute the binary cross-entropy loss over all edge types.

        Parameters
        ----------
        pred : dict
            Dictionary of predictions per message type. Each value is a tensor
            of logits for edges of that type.
        y : dict
            Dictionary of ground-truth binary labels per message type. Each
            value is a tensor of the same shape as ``pred[key]``.

        Returns
        -------
        torch.Tensor
            Scalar loss value (sum of losses across all message types).
        """
        loss = 0
        for key in pred:
            p = torch.sigmoid(pred[key])
            loss += self.loss_fn(p, y[key].type(pred[key].dtype))
        return loss

    @property
    def name(self):
        """
        str: Returns the model name, including the convolution type used.

        Returns
        -------
        str
            ``"HeteroGNN(GCNConv)"`` if using GCN-style layers,
            ``"HeteroGNN(SAGEConv)"`` if using GraphSAGE layers.
        """
        return (
            "HeteroGNN(GCNConv)"
            if isinstance(self.conv1, HeteroConv)
            else "HeteroGNN(SAGEConv)"
        )

    @staticmethod
    def generate_2convs_link_pred_layers(
        hete: nx.Graph, conv: HeteroSAGEConv, hidden_size: int
    ):
        """
        Generate two sets of convolutional layers for heterogeneous link prediction.

        Parameters
        ----------
        hete : networkx.Graph
            Heterogeneous graph with node and edge type metadata.
        conv : class
            Heterogeneous convolution layer class (e.g., HeteroSAGEConv).
        hidden_size : int
            Dimensionality of hidden representations.

        Returns
        -------
        tuple of dict
            Two dictionaries of convolutional layers keyed by message type:
            - convs1: input_dim → hidden_size
            - convs2: hidden_size → hidden_size
        """
        convs1 = {}
        convs2 = {}
        for message_type in hete.message_types:
            n_type = message_type[0]
            s_type = message_type[2]
            n_feat_dim = hete.num_node_features(n_type)
            s_feat_dim = hete.num_node_features(s_type)
            convs1[message_type] = conv(n_feat_dim, hidden_size, s_feat_dim)
            convs2[message_type] = conv(hidden_size, hidden_size, hidden_size)
        return convs1, convs2
