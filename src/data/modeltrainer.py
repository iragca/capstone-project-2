import copy

import mlflow
import torch
from tqdm import tqdm
from sklearn.metrics import roc_auc_score, f1_score


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

        for epoch in tqdm(range(1, self.args["epochs"] + 1), desc="Training Epochs", ncols=100):
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

                score_train = self.evaluate(self.dataloaders["train"])
                score_val = self.evaluate(self.dataloaders["val"])
                score_test = self.evaluate(self.dataloaders["test"])

                mlflow.log_metrics(
                    {
                        "TRAIN: ROC-AUC": score_train["roc_auc"],
                        "VAL: ROC-AUC": score_val["roc_auc"],
                        "TEST: ROC-AUC": score_test["roc_auc"],
                        "TRAIN: F1 Score": score_train["f1_score"],
                        "VAL: F1 Score": score_val["f1_score"],
                        "TEST: F1 Score": score_test["f1_score"],
                        "Loss": loss.item(),
                    },
                    step=epoch,
                )

                if val_max < score_val["roc_auc"]:
                    val_max = score_val["roc_auc"]
                    best_model = copy.deepcopy(self.model)

        return best_model

    def evaluate(self, dataloader):
        self.model.eval()
        score = 0
        f1_score_count = 0
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

            score += roc_auc_score(
                batch.edge_label.flatten().cpu().numpy(),
                pred.flatten().data.cpu().numpy(),
            )
            pred_labels = (pred > self.args["threshold"]).float()
            f1_score_count += f1_score(
                batch.edge_label.flatten().cpu().numpy(),
                pred_labels.flatten().data.cpu().numpy(),
                zero_division=0,
            )
            num_batches += 1

        return {
            "roc_auc": score / num_batches,
            "f1_score": f1_score_count / num_batches,
        }
