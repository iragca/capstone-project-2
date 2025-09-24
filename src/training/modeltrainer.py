import copy

import mlflow
import numpy as np
import torch
from sklearn.metrics import (
    accuracy_score,
    auc,
    f1_score,
    precision_recall_curve,
    precision_score,
    recall_score,
    roc_auc_score,
)
from tqdm import tqdm


class ModelTrainer:
    def __init__(self, args, dataset, model, optimizer, dataloaders):
        self.args = args
        self.dataset = dataset
        self.model = model.to(args["device"])
        self.optimizer = optimizer
        self.dataloaders = dataloaders
        self.device = args["device"]

    def train(self):
        val_max = 0
        best_model = self.model

        for epoch in tqdm(
            range(1, self.args["epochs"] + 1), desc="Training Epochs", ncols=100
        ):
            for i, batch in enumerate(self.dataloaders["train"]):
                loss = self.train_step(batch)

                metrics = {}

                for split in ["train", "val", "test"]:
                    score = self.evaluate(self.dataloaders[split])
                    metrics.update(
                        {
                            f"{split.upper()}: ROC-AUC": score["roc_auc"],
                            f"{split.upper()}: F1 Score": score["f1_score"],
                            f"{split.upper()}: Precision": score["precision"],
                            f"{split.upper()}: Recall": score["recall"],
                            f"{split.upper()}: PR-AUC": score["pr_auc"],
                            f"{split.upper()}: Accuracy": score["accuracy"],
                        }
                    )

                metrics["Loss"] = loss

                mlflow.log_metrics(metrics, step=epoch)

                if val_max < metrics["VAL: ROC-AUC"]:
                    val_max = metrics["VAL: ROC-AUC"]
                    best_model = copy.deepcopy(self.model)

        return best_model

    def evaluate(self, dataloader):
        self.model.eval()

        all_y_true = []
        all_y_pred = []

        for batch in dataloader:
            embeddings, edge_label_index = self.forward_pass(batch)
            pred = torch.sigmoid(
                self.dot_product_similarity(embeddings, edge_label_index)
            )

            y_true = batch.edge_label.flatten().cpu().numpy()
            y_pred = pred.flatten().data.cpu().numpy()

            all_y_true.append(y_true)
            all_y_pred.append(y_pred)

        all_y_true = np.concatenate(all_y_true)
        all_y_pred = np.concatenate(all_y_pred)

        y_labels_pred = (all_y_pred > self.args["threshold"]).astype(int)

        # Global metrics
        roc_auc = roc_auc_score(all_y_true, all_y_pred)
        precision, recall, _ = precision_recall_curve(all_y_true, all_y_pred)
        pr_auc = auc(recall, precision)

        return {
            "roc_auc": roc_auc,
            "pr_auc": pr_auc,
            "f1_score": f1_score(all_y_true, y_labels_pred, zero_division=0),
            "recall": recall_score(all_y_true, y_labels_pred, zero_division=0),
            "precision": precision_score(all_y_true, y_labels_pred, zero_division=0),
            "accuracy": accuracy_score(all_y_true, y_labels_pred),
        }

    def train_step(self, batch) -> float:
        self.model.train()
        self.optimizer.zero_grad()

        embeddings, edge_label_index = self.forward_pass(batch)
        pred = self.dot_product_similarity(embeddings, edge_label_index)
        loss = self.model.loss(pred, batch.edge_label.type(pred.dtype))

        loss.backward()
        self.optimizer.step()

        return loss.item()

    def forward_pass(self, batch) -> tuple[torch.Tensor, torch.Tensor]:
        batch.to(self.device)
        embeddings, edge_label_index = self.model(batch)
        return embeddings, edge_label_index

    def dot_product_similarity(self, node_embeddings, edge_label_index):
        nodes_first = torch.index_select(
            node_embeddings, 0, edge_label_index[0, :].long()
        )
        nodes_second = torch.index_select(
            node_embeddings, 0, edge_label_index[1, :].long()
        )
        return torch.sum(nodes_first * nodes_second, dim=-1)
