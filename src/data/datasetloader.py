import sys
from typing import Type, Union

import networkx as nx
import polars as pl
import torch
from pocketbase.models import Record

from ..config import DATA_DIR
from ..db import PBWarehouse
from ..utils import ensure_path


class DatasetLoader:
    def __init__(self, pb: PBWarehouse, cache_dir=DATA_DIR / ".cache"):
        self.cache_dir = cache_dir
        self.dataset_path = self.cache_dir / "dataset_cache.csv"
        self.pb = pb

        if not self.cache_dir.exists():
            ensure_path(self.cache_dir)

    def load_dataset(
        self,
        cache: bool = True,
        dtype: Type[pl.DataFrame] | Type[nx.Graph] | Type[nx.DiGraph] = pl.DataFrame,
    ) -> Union[pl.DataFrame, nx.Graph, nx.DiGraph]:
        """Load the dataset from the PocketBase warehouse."""

        if dtype not in [pl.DataFrame, nx.Graph, nx.DiGraph]:
            raise ValueError(
                "Unsupported dtype. Use pl.DataFrame or nx.Graph/nx.DiGraph."
            )

        if not self.dataset_path.exists():
            print("Dataset cache not found. Fetching from PocketBase...")
            df = self.get_dataset()
            if cache:
                self._cache_dataset(df)
        else:
            df = self.get_cached_dataset()

        if dtype == pl.DataFrame:
            return df
        else:
            return self._create_graph(data=df, dtype=dtype)

    def get_dataset(self) -> pl.DataFrame:
        """Fetch the dataset from the PocketBase warehouse."""
        print("This may take 30 minutes or more...")
        records: list[Record] = self.pb.get_dataset()
        data: list[dict] = self._parse_records(records)
        self._inline_print("Dataset fetched from the warehouse.")
        return pl.DataFrame(data)

    def get_cached_dataset(self) -> pl.DataFrame:
        """Load the cached dataset if available."""
        self._inline_print("Loading dataset from cache...")
        if self.dataset_path.exists():
            df = pl.read_csv(self.dataset_path)
            self._inline_print("Cached dataset loaded successfully.")
            return df
        else:
            raise FileNotFoundError(f"Cached dataset not found at {self.dataset_path}")

    def update_cache(self) -> pl.DataFrame:
        """Update the cached dataset."""
        print("Updating cache...")
        data: pl.DataFrame = self.get_dataset()
        self._cache_dataset(data)
        self._inline_print("Cache updated successfully.")
        return data

    def _cache_dataset(self, data: pl.DataFrame) -> None:
        """Save the dataset to the cache."""
        data.write_csv(self.dataset_path)
        print(f"Dataset cached at {self.dataset_path}")
        return None

    def _parse_records(self, records: list[Record]) -> list[dict]:
        """Parse PocketBase records to a list of dictionaries."""
        return [self._parse_record(record) for record in records]

    @staticmethod
    def _parse_record(record: Record) -> dict:
        """Remove boilerplate keys from a record.

        Args:
            record (Record): The PocketBase record to parse.

        Returns:
            dict: A dictionary representation of the record without boilerplate keys.
        """
        record = record.__dict__
        keys_to_remove = [
            "id",
            "created",
            "updated",
            "collection_id",
            "collection_name",
            "expand",
        ]

        for key in keys_to_remove:
            if key in record:
                del record[key]
        return record

    def _create_graph(
        self, data: pl.DataFrame, dtype: nx.Graph | nx.DiGraph
    ) -> nx.Graph | nx.DiGraph:
        """Create a bipartite graph from the dataset.

        Args:
            data (pl.DataFrame): The dataset to create the graph from.
            dtype (nx.Graph | nx.DiGraph): The type of graph to create.

        Returns:
            nx.Graph | nx.DiGraph: The created graph.
        """

        self._validate_graph_inputs(data, dtype)

        G: nx.Graph | nx.DiGraph = dtype()

        for row in data.iter_rows(named=True):
            tweet_id = row["tweet_id"]
            is_hateful = row["is_hateful"]
            user_id = row["user_id"]

            tweet_features = torch.tensor(
                [
                    row["favorite_count"],
                    row["retweet_count"],
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
                tweet_id,
                node_label=is_hateful,
                bipartite=0,
                node_feature=tweet_features,
            )
            G.add_node(user_id, node_label=3, bipartite=1, node_feature=user_features)
            if tweet_id and user_id:
                G.add_edge(tweet_id, user_id)

        return G

    def _validate_graph_inputs(self, data: pl.DataFrame, dtype: type) -> None:
        self._check_is_dataframe(data)
        self._check_not_empty(data)
        self._check_graph_type(dtype)
        self._check_no_nulls(data)

    def _check_no_nulls(self, data: pl.DataFrame) -> None:
        if self._has_null(data):
            raise ValueError("Dataset contains null values. Please clean the data.")

    @staticmethod
    def _check_is_dataframe(data: pl.DataFrame) -> None:
        if not isinstance(data, pl.DataFrame):
            raise TypeError("Data must be a polars DataFrame.")

    @staticmethod
    def _check_not_empty(data: pl.DataFrame) -> None:
        if data.is_empty():
            raise ValueError("Dataset is empty. Please load a valid dataset.")

    @staticmethod
    def _check_graph_type(dtype: type) -> None:
        if dtype not in (nx.Graph, nx.DiGraph):
            raise ValueError("Unsupported graph type. Use nx.Graph or nx.DiGraph.")

    @staticmethod
    def _inline_print(message: str) -> None:
        """Print a message inline."""
        sys.stdout.write(f"\r{message}")
        sys.stdout.flush()
        return None

    @staticmethod
    def _has_null(data: pl.DataFrame) -> bool:
        """Check if the DataFrame has any null values."""
        return data.null_count().to_numpy().sum() > 0
