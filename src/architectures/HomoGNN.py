import torch
import torch.nn as nn
import torch.nn.functional as F

from torch_geometric.nn import SAGEConv
from torch_geometric.nn import GCNConv


class HomoGNN(torch.nn.Module):
    def __init__(
        self, input_size: int = 128, hidden_size: int = 128, num_layers: int = 10
    ):
        super(HomoGNN, self).__init__()

        self.conv1 = SAGEConv(input_size, hidden_size)
        self.bn1 = nn.BatchNorm1d(hidden_size)
        self.loss_fn = torch.nn.BCEWithLogitsLoss()

        self.convs = nn.ModuleList()
        self.bns = nn.ModuleList()

        for _ in range(num_layers - 3):
            self.convs.append(SAGEConv(hidden_size, hidden_size))
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

        nodes_first = torch.index_select(x, 0, edge_label_index[0, :].long())
        nodes_second = torch.index_select(x, 0, edge_label_index[1, :].long())
        pred = torch.sum(nodes_first * nodes_second, dim=-1)
        return pred

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
