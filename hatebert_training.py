import argparse
import copy
import logging
import warnings

import mlflow
import networkx as nx
import numpy as np
import polars as pl
import torch
from deepsnap.batch import Batch
from deepsnap.dataset import GraphDataset
from deepsnap.hetero_graph import HeteroGraph
from mlflow.models.signature import ModelSignature
from mlflow.types.schema import Schema, TensorSpec
from sklearn.metrics import f1_score, roc_auc_score
from torch.utils.data import DataLoader
from torchinfo import summary
from tqdm import tqdm

from src.architectures import HeteroGNN, HomoGNN
from src.data import DatasetLoader, GraphBuilder, Preprocessor
from src.db import PBWarehouse
from src.models import Features


def arg_parser():
    parser = argparse.ArgumentParser(description="Link pred arguments.")
    parser.add_argument(
        "--epochs", type=int, default=50, help="Number of epochs to train."
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
    parser.add_argument(
        "--num_layers",
        type=int,
        default=10,
        help="Number of layers in the model.",
    )
    parser.add_argument(
        "--heterogeneous",
        action="store_true",
        default=False,
        help="Whether to use a heterogeneous graph.",
    )
    parser.add_argument(
        "--directed",
        action="store_true",
        default=False,
        help="Whether to use a directed graph.",
    )
    parser.add_argument(
        "--c",
        action="store_true",
        default=False,
        help="Flag if you are configuring/developing/testing this script",
    )
    parser.add_argument(
        "--save-model",
        action="store_true",
        default=False,
        help="Flag to save the best model after training.",
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

    dataset_loader = DatasetLoader(PBWarehouse())
    data: pl.DataFrame = dataset_loader.load_dataset()
    preprocessor = Preprocessor(data=data)
    data = preprocessor.preprocess()

    node_features = Features(
        tweet=[
            "favorite_count",
            # "retweet_count",
            # "bookmark_count",
            "reply_count",
            "quote_count",
            # "views",
            "source",
            "is_hateful",
        ],
        user=[
            # "favourites_count",
            # "follower_count",
            # "following_count",
            # "number_of_tweets",
            # "listed_count",
            # "is_blue_verified",
            "friends",
        ],
    )

    gb = GraphBuilder(data=data, node_features=node_features)

    graph: nx.DiGraph | nx.Graph = gb.create_graph(directed=args["directed"])

    dataset = GraphDataset([graph], task="link_pred", edge_train_mode="disjoint")

    datasets = {}
    datasets["train"], datasets["val"], datasets["test"] = dataset.split(
        transductive=True, split_ratio=[0.85, 0.05, 0.1]
    )
    input_dim = datasets["train"].num_node_features
    # num_classes = datasets["train"].num_edge_labels

    if args["heterogeneous"]:
        graph = HeteroGraph(graph)
        model = HeteroGNN(hetero=graph, hidden_size=args["hidden_dim"]).to(
            args["device"]
        )
    else:
        model = HomoGNN(
            input_size=input_dim,
            hidden_size=args["hidden_dim"],
            num_layers=args["num_layers"],
        ).to(args["device"])

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

    features_selected = node_features.tweet + node_features.user
    mlflow_dataset = mlflow.data.from_pandas(
        data.select(features_selected).to_pandas(),
        name="HateBERT Dataset",
    )

    # Suppress warnings
    logging.getLogger("mlflow.system_metrics.metrics.gpu_monitor").setLevel(
        logging.ERROR
    )
    logging.getLogger("mlflow.utils.requirements_utils").setLevel(logging.ERROR)
    logging.getLogger("mlflow.utils.environment").setLevel(logging.ERROR)
    with mlflow.start_run(log_system_metrics=True):
        mlflow.log_params(args)
        mlflow.log_input(
            mlflow_dataset, context="training", tags={"source": "X / Twitter"}
        )
        mlflow.set_tag("purpose", "configuration" if args["c"] else "training")
        mlflow.set_tag("framework", "pytorch")
        mlflow.set_tag("task", "link prediction")
        mlflow.set_tag("nature", "transductive")

        try:
            best_model = train(model, dataloaders, optimizer, args)
        except Exception as e:
            with open("error_log.txt", "w") as f:
                f.write(str(e))
            mlflow.log_artifact("error_log.txt")
            mlflow.set_tag("error", "Training failed")
            raise

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

        mlflow.log_artifact("model_summary.txt")
        mlflow.log_artifact("model_architecture.txt")

        input_schema = Schema(
            [
                TensorSpec(
                    type=np.dtype(np.float32),
                    shape=(-1, input_dim),
                    name="node_feature",
                ),
                TensorSpec(type=np.dtype(np.int64), shape=(2, -1), name="edge_index"),
                TensorSpec(
                    type=np.dtype(np.int64), shape=(2, -1), name="edge_label_index"
                ),
            ]
        )
        output_schema = Schema(
            [TensorSpec(type=np.dtype(np.float32), shape=(-1,), name="logits")]
        )
        signature = ModelSignature(inputs=input_schema, outputs=output_schema)
        mlflow.pytorch.log_model(
            best_model,
            name=model.name,
            signature=signature,
        )

        best_train_scores = test(best_model, dataloaders["train"], args)
        best_val_scores = test(best_model, dataloaders["val"], args)
        best_test_scores = test(best_model, dataloaders["test"], args)

        print(
            f"Best Train ROC AUC: {best_train_scores['roc_auc']:.4f}, "
            f"Best Val ROC AUC: {best_val_scores['roc_auc']:.4f}, "
            f"Best Test ROC AUC: {best_test_scores['roc_auc']:.4f}, "
            f"Best Test F1 Score: {best_test_scores['f1_score']:.4f} "
        )

        if args["save_model"]:
            torch.save(best_model.state_dict(), "best_model.pth")
            mlflow.log_artifact("best_model.pth")


def train(model, dataloaders, optimizer, args, print_progress=False):
    val_max = 0
    best_model = model

    for epoch in tqdm(range(1, args["epochs"] + 1), desc="Training Epochs", ncols=100):
        for i, batch in enumerate(dataloaders["train"]):
            batch.to(args["device"])
            model.train()
            optimizer.zero_grad()
            embeddings, edge_label_index = model(batch)

            nodes_first = torch.index_select(embeddings, 0, edge_label_index[0, :].long())
            nodes_second = torch.index_select(embeddings, 0, edge_label_index[1, :].long())
            pred = torch.sum(nodes_first * nodes_second, dim=-1)

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
        embeddings, edge_label_index = model(batch)
        nodes_first = torch.index_select(embeddings, 0, edge_label_index[0, :].long())
        nodes_second = torch.index_select(embeddings, 0, edge_label_index[1, :].long())
        pred = torch.sum(nodes_first * nodes_second, dim=-1)
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


if __name__ == "__main__":
    main()
