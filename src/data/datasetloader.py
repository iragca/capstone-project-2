from typing import Type, Union

import networkx as nx
import polars as pl
from pocketbase.models import Record

from ..config import DATA_DIR
from ..db import PBWarehouse
from ..utils import ensure_path, function_printer, inline_print


class DatasetLoader:
    def __init__(self, pb: PBWarehouse, cache_dir=DATA_DIR / ".cache"):
        self.cache_dir = cache_dir
        self.dataset_path = self.cache_dir / "dataset_cache.csv"
        self.pb = pb

        if not self.cache_dir.exists():
            ensure_path(self.cache_dir)

    @function_printer("Loading dataset")
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
            inline_print("Dataset cache not found.\n")
            df = self.get_dataset()
            if cache:
                self._cache_dataset(df)
        else:
            df = self.get_cached_dataset()

        if dtype == pl.DataFrame:
            return df
        else:
            return self._create_graph(data=df, dtype=dtype)

    @function_printer("Fetching data (may take a few minutes or more)")
    def get_dataset(self) -> pl.DataFrame:
        """Fetch the dataset from the PocketBase warehouse."""
        records: list[Record] = self.pb.get_dataset()
        data: list[dict] = self._parse_records(records)
        return pl.DataFrame(data)

    @function_printer("Loading cached dataset")
    def get_cached_dataset(self) -> pl.DataFrame:
        """Load the cached dataset if available."""
        if self.dataset_path.exists():
            df = pl.read_csv(self.dataset_path)
            return df
        else:
            raise FileNotFoundError(f"Cached dataset not found at {self.dataset_path}")

    @function_printer("Updating cache")
    def update_cache(self) -> pl.DataFrame:
        """Update the cached dataset."""
        data: pl.DataFrame = self.get_dataset()
        self._cache_dataset(data)
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
