import torch
import torch.nn as nn
import torch.nn.functional as F

from torch_geometric.nn import SAGEConv
from torch_geometric.nn import GCNConv

class HomoGNN(torch.nn.Module):
    def __init__(
        self, input_size: int = 128, hidden_size: int = 128, num_layers: int = 8, GraphSAGE: bool = True
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
        return self.loss_fn(pred, label)

    @property
    def name(self):
        """Returns the name of the model."""
        return (
            "HomoGNN(GCNConv)"
            if isinstance(self.conv1, GCNConv)
            else "HomoGNN(SAGEConv)"
        )
