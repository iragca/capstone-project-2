import argparse
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
from torch.utils.data import DataLoader
from torchinfo import summary

from src.architectures import HeteroGNN, HomoGNN
from src.data import DatasetLoader, GraphBuilder, Preprocessor
from src.db import PBWarehouse
from src.models import Features
from src.training import ModelTrainer
from src.config import Settings


def arg_parser():
    parser = argparse.ArgumentParser(description="Link pred arguments.")
    parser.add_argument(
        "--epochs", type=int, default=50, help="Number of epochs to train."
    )
    parser.add_argument(
        "--hidden_dim", type=int, default=24, help="Hidden dimension of the model."
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
        default=8,
        help="Number of layers in the model.",
    )
    parser.add_argument(
        "--homogeneous",
        action="store_true",
        default=False,
        help="Whether to use a homogeneous graph.",
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
    parser.add_argument(
        "--graphsage",
        action="store_true",
        default=False,
        help="Flag to use GraphSAGE convolutional layers instead of GCN.",
    )
    parser.add_argument(
        "--experiment-name",
        type=str,
        default="Random Experiment",
        help="Name of the MLflow experiment.",
    )
    parser.add_argument(
        "--learning-rate", type=float, default=0.1, help="Learning rate for the optimizer."
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
    mlflow.set_tracking_uri(uri=Settings.MLFLOW_TRACKING_URI.value)
    mlflow.set_experiment(f"[CAPSTONE-2] {args['experiment_name']}")

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

    if args["homogeneous"]:
        model = HomoGNN(
            input_size=input_dim,
            hidden_size=args["hidden_dim"],
            num_layers=args["num_layers"],
            GraphSAGE=args["graphsage"],
        ).to(args["device"])
    else:
        graph = HeteroGraph(graph)
        model = HeteroGNN(hetero=graph, hidden_size=args["hidden_dim"]).to(
            args["device"]
        )

    optimizer = torch.optim.SGD(
        model.parameters(), lr=0.01, momentum=0.9, weight_decay=5e-4
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
        mlflow.set_tag("purpose", "finding the standard error")
        mlflow.set_tag("framework", "pytorch")
        mlflow.set_tag("task", "link prediction")
        mlflow.set_tag("nature", "transductive")

        trainer = ModelTrainer(
            args=args,
            dataset=dataset,
            model=model,
            optimizer=optimizer,
            dataloaders=dataloaders,
        )

        try:
            best_model = trainer.train()
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

        best_train_scores = trainer.evaluate(dataloaders["train"])
        best_val_scores = trainer.evaluate(dataloaders["val"])
        best_test_scores = trainer.evaluate(dataloaders["test"])

        print(
            f"Best Train ROC AUC: {best_train_scores['roc_auc']:.4f}, "
            f"Best Val ROC AUC: {best_val_scores['roc_auc']:.4f}, "
            f"Best Test ROC AUC: {best_test_scores['roc_auc']:.4f}, "
            f"Best Test F1 Score: {best_test_scores['f1_score']:.4f} "
        )

        if args["save_model"]:
            torch.save(best_model.state_dict(), "best_model.pth")
            mlflow.log_artifact("best_model.pth")


if __name__ == "__main__":
    main()
