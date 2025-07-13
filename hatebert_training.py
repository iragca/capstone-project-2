import copy
import warnings

import argparse
import mlflow
import networkx as nx
import polars as pl
import torch
from deepsnap.batch import Batch
from deepsnap.dataset import GraphDataset
from sklearn.metrics import f1_score, roc_auc_score
from torch.utils.data import DataLoader
from torchinfo import summary
from tqdm import tqdm

from src.architectures.LinkPrediction import HateBERT
from src.config import PROCESSED_DATA_DIR


def arg_parser():
    parser = argparse.ArgumentParser(description="Link pred arguments.")
    parser.add_argument(
        "--epochs", type=int, default=150, help="Number of epochs to train."
    )
    parser.add_argument(
        "--hidden_dim", type=int, default=128, help="Hidden dimension of the model."
    )
    parser.add_argument(
        "--threshold", type=float, default=0.5, help="Threshold for classification."
    )
    parser.add_argument(
        "--device",
        type=str,
        default="cuda" if torch.cuda.is_available() else "cpu",
        help="Device to use for training.",
    )

    args = parser.parse_args()

    assert args.epochs > 0, "Number of epochs must be greater than 0"
    assert args.hidden_dim > 0, "Hidden dimension must be greater than 0"
    assert 0 <= args.threshold <= 1, "Threshold must be between 0 and 1"
    assert args.device in ["cpu", "cuda"], "Device must be either 'cpu' or 'cuda'"

    if args.device == "cuda" and not torch.cuda.is_available():
        raise ValueError(
            "CUDA is not available on this machine. Please use 'cpu' instead."
        )

    return args.__dict__


def main():
    args = arg_parser()

    warnings.filterwarnings("ignore")
    mlflow.set_tracking_uri(uri="http://192.168.100.203:5000/")
    mlflow.set_experiment("[CAPSTONE-2] Link Prediction")
    data: pl.DataFrame = load_data()
    graph: nx.DiGraph = create_graph(data)
    dataset = GraphDataset([graph], task="link_pred", edge_train_mode="disjoint")

    datasets = {}
    datasets["train"], datasets["val"], datasets["test"] = dataset.split(
        transductive=True, split_ratio=[0.85, 0.05, 0.1]
    )
    input_dim = datasets["train"].num_node_features
    # num_classes = datasets["train"].num_edge_labels

    model = HateBERT(input_dim, args["hidden_dim"]).to(args["device"])

    optimizer = torch.optim.SGD(
        model.parameters(), lr=0.1, momentum=0.9, weight_decay=5e-4
    )

    dataloaders = {
        split_name: DataLoader(
            split_dataset,
            collate_fn=Batch.collate([]),
            batch_size=1,
            shuffle=(split_name == "train"),
        )
        for split_name, split_dataset in datasets.items()
    }

    mlflow_dataset = mlflow.data.from_pandas(
        data.to_pandas(),
        name="HateBERT Dataset",
    )

    with mlflow.start_run(log_system_metrics=True):
        mlflow.log_params(args)
        mlflow.log_input(
            mlflow_dataset, context="training", tags={"source": "X / Twitter"}
        )
        mlflow.set_tag("purpose", "training")
        mlflow.set_tag("framework", "pytorch")
        mlflow.set_tag("task", "link prediction")
        mlflow.set_tag("nature", "transductive")
        best_model = train(model, dataloaders, optimizer, args)

        with open("model_summary.txt", "w") as f:
            f.write(
                str(
                    summary(
                        best_model,
                        device=args["device"],
                        col_names=[
                            "num_params",
                            "params_percent",
                            "kernel_size",
                        ],
                    )
                )
            )

        with open("model_architecture.txt", "w") as f:
            f.write(str(best_model))
        # graph_path = "graph_snapshot.graphml"
        # nx.write_graphml(graph, graph_path)

        mlflow.log_artifact("model_summary.txt")
        mlflow.log_artifact("model_architecture.txt")
        # mlflow.log_artifact(graph_path)

        best_train_scores = test(best_model, dataloaders["train"], args)
        best_val_scores = test(best_model, dataloaders["val"], args)
        best_test_scores = test(best_model, dataloaders["test"], args)

        print(
            f"Best Train ROC AUC: {best_train_scores['roc_auc']:.4f}, "
            f"Best Val ROC AUC: {best_val_scores['roc_auc']:.4f}, "
            f"Best Test ROC AUC: {best_test_scores['roc_auc']:.4f}"
            f"Best Test F1 Score: {best_test_scores['f1_score']:.4f}, "
        )


def train(model, dataloaders, optimizer, args, print_progress=False):
    val_max = 0
    best_model = model

    for epoch in tqdm(range(1, args["epochs"] + 1), desc="Training Epochs", ncols=100):
        for i, batch in enumerate(dataloaders["train"]):
            batch.to(args["device"])
            model.train()
            optimizer.zero_grad()
            pred = model(batch)
            loss = model.loss(pred, batch.edge_label.type(pred.dtype))
            # print(pred[0], batch.edge_label.type(pred.dtype)[0])
            loss.backward()
            optimizer.step()

            score_train = test(model, dataloaders["train"], args)
            score_val = test(model, dataloaders["val"], args)
            score_test = test(model, dataloaders["test"], args)

            if print_progress:
                print(
                    f"Epoch: {epoch:03d}, Batch: {i:03d}, "
                    f"Train ROC AUC: {score_train['roc_auc']:.4f}, "
                    f"Val ROC AUC: {score_val['roc_auc']:.4f}, "
                    f"Val F1 Score: {score_val['f1_score']:.4f}, "
                    f"Test ROC AUC: {score_test['roc_auc']:.4f}, "
                    f"Test F1 Score: {score_test['f1_score']:.4f}, "
                    f"Loss: {loss.item():.5f}"
                )

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
                best_model = copy.deepcopy(model)
    return best_model


def test(model, dataloader, args):
    model.eval()
    score = 0
    f1_score_count = 0
    num_batches = 0
    for batch in dataloader:
        batch.to(args["device"])
        pred = model(batch)
        pred = torch.sigmoid(pred)
        score += roc_auc_score(
            batch.edge_label.flatten().cpu().numpy(), pred.flatten().data.cpu().numpy()
        )
        pred_labels = (pred > args["threshold"]).float()
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


def load_data() -> pl.DataFrame:
    data = pl.read_csv(PROCESSED_DATA_DIR / "hatebert-test_with_users.csv")
    return data


def create_graph(data: pl.DataFrame) -> nx.Graph:
    G = nx.DiGraph()

    for row in data.iter_rows(named=True):
        tweet_id = row["tweet_id"]
        is_hateful = row["is_hateful"]
        user_id = row["user_id"]

        tweet_features = torch.tensor(
            [
                row["retweet_count"],
                row["favorite_count"],
                row["bookmark_count"],
                row["reply_count"],
                row["quote_count"],
                row["views"],
                row["is_hateful"],
            ],
            dtype=torch.float32,
        )

        user_features = torch.tensor(
            [
                row["favourites_count"],
                row["follower_count"],
                row["following_count"],
                row["number_of_tweets"],
                row["listed_count"],
                row["is_blue_verified"],
                0,
            ],
            dtype=torch.float32,
        )

        G.add_node(
            tweet_id, node_label=is_hateful, bipartite=0, node_feature=tweet_features
        )
        G.add_node(user_id, node_label=3, bipartite=1, node_feature=user_features)
        if tweet_id and user_id:
            G.add_edge(tweet_id, user_id)

    return G


if __name__ == "__main__":
    main()
