import subprocess

import torch
from typer import Typer

from src.config import (
    LOGGER_DIR,
    PROJECT_ROOT,
    logger,
)
from src.data import DatasetLoader
from src.db import PBWarehouse
from src.utils import function_logger

cli = Typer()


def grid_search(script: str = "hatebert") -> None:
    """Run grid search for hyperparameter tuning."""
    logger.add(PROJECT_ROOT / "reports" / "logs" / "grid_search.logs")
    logger.info(f"Running grid search for script: {script}")

    if script == "hatebert":
        for threshold in range(1, 10):
            subprocess.run(
                [
                    "uv",
                    "run",
                    "hatebert_training.py",
                    "--threshold",
                    str(threshold / 10),
                ],
            )
        logger.info("Completed grid search.")
    else:
        logger.error(f"Unknown script: {script}. Please use 'hatebert'.")


@cli.command()
@function_logger(LOGGER_DIR=LOGGER_DIR)
def install_torch_geometric_dependencies() -> None:
    """Install necessary dependencies for PyTorch Geometric."""

    torch_version = str(torch.__version__)
    scatter_src = f"https://pytorch-geometric.com/whl/torch-{torch_version}.html"
    sparse_src = f"https://pytorch-geometric.com/whl/torch-{torch_version}.html"

    subprocess.run(
        ["uv", "pip", "install", "torch-scatter", "-f", scatter_src],
        check=True,
    )
    subprocess.run(
        ["uv", "pip", "install", "torch-sparse", "-f", sparse_src],
        check=True,
    )


@cli.command()
@function_logger(LOGGER_DIR=LOGGER_DIR)
def update_dataset_cache() -> None:
    """Update the dataset cache."""
    dataset_loader = DatasetLoader(PBWarehouse())
    dataset_loader.update_cache()


@cli.command()
@function_logger(LOGGER_DIR=LOGGER_DIR)
def multiple_training(trials: int = 50) -> None:
    """Run the HateBERT training multiple times.
    Often done to find the standard error of a statistic.
    """
    # TODO: make this like python *kwargs style
    # >>> python script.py learning_rate=0.01 batch_size=32
    # >> kwargs = dict(arg.split("=") for arg in sys.argv[1:])
    # >> print(kwargs)  # {'learning_rate': '0.01', 'batch_size': '32'}

    for trial in range(trials):
        subprocess.run(
            [
                "uv",
                "run",
                "hatebert_training.py",
                "--hidden_dim",
                "24",
                "--epochs",
                "200",
            ],
            check=True,
        )


if __name__ == "__main__":
    cli()
