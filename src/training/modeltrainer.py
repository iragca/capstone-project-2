import copy

import mlflow
import torch
from sklearn.metrics import auc, f1_score, precision_score, recall_score, roc_auc_score
from tqdm import tqdm


class ModelTrainer:
    def __init__(self, args, dataset, model, optimizer, dataloaders):
        self.args = args
        self.dataset = dataset
        self.model = model
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
                batch.to(self.device)
                self.model.train()
                self.optimizer.zero_grad()
                embeddings, edge_label_index = self.model(batch)

                nodes_first = torch.index_select(
                    embeddings, 0, edge_label_index[0, :].long()
                )
                nodes_second = torch.index_select(
                    embeddings, 0, edge_label_index[1, :].long()
                )
                pred = torch.sum(nodes_first * nodes_second, dim=-1)

                loss = self.model.loss(pred, batch.edge_label.type(pred.dtype))
                loss.backward()
                self.optimizer.step()


                metrics = {}

                for split in ["train", "val", "test"]:
                    score = self.evaluate(self.dataloaders[split])
                    metrics.update({
                        f"{split.upper()}: ROC-AUC": score["roc_auc"],
                        f"{split.upper()}: F1 Score": score["f1_score"],
                        f"{split.upper()}: Precision": score["precision"],
                        f"{split.upper()}: Recall": score["recall"],
                        f"{split.upper()}: AUC": score["auc"]
                    })

                metrics["Loss"] = loss.item()

                mlflow.log_metrics(metrics, step=epoch)

                if val_max < metrics["VAL: ROC-AUC"]:
                    val_max = metrics["VAL: ROC-AUC"]
                    best_model = copy.deepcopy(self.model)

        return best_model

    def evaluate(self, dataloader):
        self.model.eval()
        score = 0
        f1_score_count = 0
        recall_score_count = 0
        precision_score_count = 0
        auc_score_count = 0
        num_batches = 0

        for batch in dataloader:
            batch.to(self.device)
            embeddings, edge_label_index = self.model(batch)
            nodes_first = torch.index_select(
                embeddings, 0, edge_label_index[0, :].long()
            )
            nodes_second = torch.index_select(
                embeddings, 0, edge_label_index[1, :].long()
            )
            pred = torch.sum(nodes_first * nodes_second, dim=-1)
            pred = torch.sigmoid(pred)

            y_true = batch.edge_label.flatten().cpu().numpy()
            y_pred = pred.flatten().data.cpu().numpy()
            pred_labels = (pred > self.args["threshold"]).float()
            y_labels_pred = pred_labels.flatten().data.cpu().numpy()

            score += roc_auc_score(
                y_true,
                y_pred,
            )
            auc_score_count += auc(
                y_true,
                y_pred,
            )
            f1_score_count += f1_score(
                y_true,
                y_labels_pred,
                zero_division=0,
            )
            recall_score_count += recall_score(
                y_true,
                y_labels_pred,
                zero_division=0,
            )
            precision_score_count += precision_score(
                y_true,
                y_labels_pred,
                zero_division=0,
            )
            num_batches += 1

        return {
            "roc_auc": score / num_batches,
            "f1_score": f1_score_count / num_batches,
            "recall": recall_score_count / num_batches,
            "precision": precision_score_count / num_batches,
            "auc": auc_score_count / num_batches,
        }
