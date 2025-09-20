import subprocess

import torch
from pocketbase.errors import ClientResponseError
from pocketbase.models.record import Record
from typer import Typer

from src.config import (
    LOGGER_DIR,
    PROJECT_ROOT,
    logger,
)
from src.data import DatasetLoader
from src.db import PBWarehouse
from src.models import Tweet
from src.utils import function_logger

cli = Typer()


@cli.command()
@function_logger(LOGGER_DIR=LOGGER_DIR, level="WARNING")
def classify_data(collection: str = "tweets_v2") -> None:
    """
    Classify tweets in the specified PocketBase collection using HateBERT.

    This function retrieves tweets without a classification from the given collection,
    processes the text through a pre-trained HateBERT model, and updates each tweet's
    `is_hateful` field with the predicted class.

    Parameters
    ----------
    collection : str, optional
        The name of the PocketBase collection to classify (default is `"tweets_v2"`).

    Returns
    -------
    None
        Performs side effects:
        - Fetches tweets without classification from the specified collection.
        - Classifies each tweet as hateful or not using HateBERT.
        - Updates the PocketBase record with the predicted class.
        - Logs progress, warnings, and errors.

    Raises
    ------
    ClientResponseError
        If the PocketBase client fails to fetch or update a record.
    Exception
        For unexpected errors during model inference or data processing.

    Examples
    --------
    Run as a CLI command:

    >>> uv run scraping classify-data
    """
    from transformers import AutoModelForSequenceClassification, AutoTokenizer

    pb = PBWarehouse()

    model_name = "Hate-speech-CNERG/bert-base-uncased-hatexplain"

    tokenizer = AutoTokenizer.from_pretrained(model_name)
    model = AutoModelForSequenceClassification.from_pretrained(model_name)

    def classify_text(text: str) -> int:
        inputs = tokenizer(text, return_tensors="pt", truncation=True)
        outputs = model(**inputs)
        logits = outputs.logits
        probs = torch.nn.functional.softmax(logits, dim=-1)
        predicted_class = torch.argmax(probs, dim=-1)
        return predicted_class.item()

    have_data = True

    try:
        while have_data:
            tweetRecord = pb.get_tweet_with_no_classification(collection=collection)
            tweetRecord: Record = pb.client.collection("tweets_v2").get_one(
                tweetRecord.id
            )
            tweet = Tweet(**tweetRecord.__dict__)
            logger.info(f"Classifying tweet: {tweet.tweet_id}")
            text_class = classify_text(tweet.text)
            pb.client.collection("tweets_v2").update(
                tweetRecord.id,
                {
                    "is_hateful": text_class,
                },
            )
            logger.success(
                f"Classified tweet {tweet.tweet_id} with class {text_class}."
            )
    except ClientResponseError as e:
        if "The requested resource wasn't found." in str(e):
            logger.info("No more tweets to classify.")
            have_data = False
    except Exception as e:
        logger.error(f"Error classifying tweet: {type(e).__name__} - {e}")
        have_data = False
        return


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
def install_torch_geometric_dependencies(pip: bool = False) -> None:
    """Install necessary dependencies for PyTorch Geometric."""

    torch_version = str(torch.__version__)
    scatter_src = f"https://pytorch-geometric.com/whl/torch-{torch_version}.html"
    sparse_src = f"https://pytorch-geometric.com/whl/torch-{torch_version}.html"

    if pip:
        logger.info("Installing via pip.")
        subprocess.run(
            ["pip", "install", "torch-scatter", "-f", scatter_src],
            check=True,
        )
        subprocess.run(
            ["pip", "install", "torch-sparse", "-f", sparse_src],
            check=True,
        )
        return
    
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
