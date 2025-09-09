import torch
import torch.nn as nn
import torch.nn.functional as F

from torch_geometric.nn import SAGEConv
from torch_geometric.nn import GCNConv


class HomoGNN(torch.nn.Module):
    """
    Homogeneous Graph Neural Network (GNN) model using either GraphSAGE or GCN layers.

    This model applies multiple message-passing convolutional layers (SAGEConv or GCNConv)
    followed by batch normalization and ReLU activation. It is designed for link prediction
    or node classification tasks on homogeneous graphs.

    Parameters
    ----------
    input_size : int, default=128
        Dimensionality of input node features.
    hidden_size : int, default=128
        Dimensionality of hidden representations.
    num_layers : int, default=8
        Total number of convolutional layers (including the first one).
    GraphSAGE : bool, default=True
        If True, use :class:`torch_geometric.nn.SAGEConv`. Otherwise, use
        :class:`torch_geometric.nn.GCNConv`.

    Attributes
    ----------
    conv1 : SAGEConv or GCNConv
        The first graph convolutional layer.
    bn1 : nn.BatchNorm1d
        Batch normalization applied after the first convolution.
    convs : nn.ModuleList
        List of subsequent convolutional layers.
    bns : nn.ModuleList
        List of batch normalization layers corresponding to ``convs``.
    loss_fn : torch.nn.BCEWithLogitsLoss
        Binary cross-entropy loss function with logits, typically used for link prediction.

    Examples
    --------
    >>> from torch_geometric.data import Data
    >>> x = torch.randn(100, 128)  # node features
    >>> edge_index = torch.randint(0, 100, (2, 500))  # edges
    >>> edge_label_index = torch.randint(0, 100, (2, 200))  # candidate edges
    >>> batch = type("Batch", (), {
    ...     "node_feature": x,
    ...     "edge_index": edge_index,
    ...     "edge_label_index": edge_label_index
    ... })()
    >>> model = HomoGNN(input_size=128, hidden_size=64, num_layers=4, GraphSAGE=True)
    >>> out, edge_label_index = model(batch)
    >>> out.shape
    torch.Size([100, 64])
    """

    def __init__(
        self,
        input_size: int = 128,
        hidden_size: int = 128,
        num_layers: int = 8,
        GraphSAGE: bool = True,
    ):
        super(HomoGNN, self).__init__()

        if GraphSAGE:
            conv_layer = SAGEConv
        else:
            conv_layer = GCNConv

        self.conv1 = conv_layer(input_size, hidden_size)
        self.bn1 = nn.BatchNorm1d(hidden_size)
        self.loss_fn = torch.nn.BCEWithLogitsLoss()

        self.convs = nn.ModuleList()
        self.bns = nn.ModuleList()

        for _ in range(num_layers - 1):
            self.convs.append(conv_layer(hidden_size, hidden_size))
            self.bns.append(nn.BatchNorm1d(hidden_size))

    def forward(self, batch):
        """
        Forward pass of the GNN.

        Parameters
        ----------
        batch : object
            A batch object containing:
            - ``node_feature`` : torch.Tensor of shape (num_nodes, input_size)
              Node feature matrix.
            - ``edge_index`` : torch.LongTensor of shape (2, num_edges)
              Graph connectivity in COO format.
            - ``edge_label_index`` : torch.LongTensor of shape (2, num_labels)
              Edge indices for link prediction.

        Returns
        -------
        x : torch.Tensor
            Node embeddings of shape (num_nodes, hidden_size).
        edge_label_index : torch.LongTensor
            Candidate edges used for link prediction.
        """
        x, edge_index, edge_label_index = (
            batch.node_feature,
            batch.edge_index,
            batch.edge_label_index,
        )
        x = self.conv1(x, edge_index)
        x = self.bn1(x)
        x = F.relu(x)

        for i, conv in enumerate(self.convs):
            x = conv(x, edge_index)
            x = self.bns[i](x)
            x = F.relu(x)

        return x, edge_label_index

    def loss(self, pred, label):
        """
        Compute the binary cross-entropy loss.

        Parameters
        ----------
        pred : torch.Tensor
            Predicted logits for edges or nodes.
        label : torch.Tensor
            Ground-truth binary labels.

        Returns
        -------
        torch.Tensor
            Scalar loss value.
        """
        return self.loss_fn(pred, label)

    @property
    def name(self):
        """
        str: Returns the model name, including which convolution is used.

        Returns
        -------
        str
            ``"HomoGNN(GCNConv)"`` if using GCN layers,
            ``"HomoGNN(SAGEConv)"`` if using GraphSAGE layers.
        """
        return (
            "HomoGNN(GCNConv)"
            if isinstance(self.conv1, GCNConv)
            else "HomoGNN(SAGEConv)"
        )
