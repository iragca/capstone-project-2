from typing import Type, Union

import networkx as nx
import polars as pl
from pocketbase.models import Record

from ..config import DATA_DIR
from ..db import PBWarehouse
from ..utils import ensure_path, function_printer, inline_print


class DatasetLoader:
    """
    Utility class for loading, caching, and parsing datasets from a PocketBase warehouse.

    Parameters
    ----------
    pb : PBWarehouse, optional
        An instance of PBWarehouse for fetching data from PocketBase. If not provided,
        certain methods will raise an error when trying to fetch the dataset.
    cache_dir : Path, optional
        Directory where the dataset cache is stored (default is `DATA_DIR/.cache`).

    Attributes
    ----------
    cache_dir : Path
        Directory where the dataset cache is stored.
    dataset_path : Path
        Path to the cached dataset CSV file.
    pb : PBWarehouse
        Instance of PBWarehouse used to fetch data from PocketBase.

    Methods
    -------
    load_dataset(cache=True, dtype=pl.DataFrame | nx.Graph | nx.DiGraph) -> pl.DataFrame | nx.Graph | nx.DiGraph
        Load the dataset from cache or PocketBase and optionally cache it.
    get_dataset() -> pl.DataFrame
        Fetch the dataset from PocketBase and return as a Polars DataFrame.
    get_cached_dataset() -> pl.DataFrame
        Load the cached dataset from disk.
    update_cache() -> pl.DataFrame
        Fetch the latest dataset from PocketBase and update the cache.
    _cache_dataset(data: pl.DataFrame) -> None
        Save the provided DataFrame to the cache directory.
    _parse_records(records: list[Record]) -> list[dict]
        Parse a list of PocketBase records into dictionaries.
    _parse_record(record: Record) -> dict
        Parse a single PocketBase record into a dictionary, removing boilerplate keys.

    Raises
    ------
    ValueError
        If an unsupported dtype is provided to `load_dataset`.
    FileNotFoundError
        If the dataset or cache is not found and cannot be loaded.
    """

    def __init__(self, pb: PBWarehouse = None, cache_dir=DATA_DIR / ".cache"):
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
        """
        Load the dataset from the PocketBase warehouse or from cache.

        Parameters
        ----------
        cache : bool, optional
            Whether to cache the dataset locally after loading (default is True).
        dtype : type, optional
            Type of object to return:
            - `pl.DataFrame` for a Polars DataFrame
            - `nx.Graph` or `nx.DiGraph` for a NetworkX graph (default is `pl.DataFrame`).

        Returns
        -------
        Union[pl.DataFrame, nx.Graph, nx.DiGraph]
            The dataset as either a Polars DataFrame or a NetworkX graph.

        Raises
        ------
        ValueError
            If `dtype` is not supported.
        FileNotFoundError
            If the dataset is not found locally and no PBWarehouse instance is provided.
        """

        if dtype not in [pl.DataFrame, nx.Graph, nx.DiGraph]:
            raise ValueError(
                "Unsupported dtype. Use pl.DataFrame or nx.Graph/nx.DiGraph."
            )

        dataset_exists = self.dataset_path.exists()
        db_exists = self.pb is not None

        if not dataset_exists and db_exists:
            inline_print("Dataset cache not found.\n")
            df = self.get_dataset()
            if cache:
                self._cache_dataset(df)
        elif dataset_exists:
            df = self.get_cached_dataset()
        else:
            raise FileNotFoundError(
                "Dataset not found. Make sure to pass a valid PBWarehouse to fetch the dataset."
            )

        if dtype == pl.DataFrame:
            return df
        else:
            return self._create_graph(data=df, dtype=dtype)

    @function_printer("Fetching data (may take a few minutes or more)")
    def get_dataset(self, collection: str = "dataset") -> pl.DataFrame:
        """
        Fetch the dataset from the PocketBase warehouse.

        Returns
        -------
        pl.DataFrame
            The dataset as a Polars DataFrame.
        """
        if self.pb is None:
            raise ValueError("PBWarehouse instance not provided.")

        records: list[Record] = self.pb.get_dataset(collection=collection)
        data: list[dict] = self._parse_records(records)
        return pl.DataFrame(data)

    @function_printer("Loading cached dataset")
    def get_cached_dataset(self) -> pl.DataFrame:
        """
        Load the cached dataset from disk.

        Returns
        -------
        pl.DataFrame
            The cached dataset as a Polars DataFrame.

        Raises
        ------
        FileNotFoundError
            If the cached dataset does not exist.
        """
        if self.dataset_path.exists():
            df = pl.read_csv(self.dataset_path)
            return df
        else:
            raise FileNotFoundError(f"Cached dataset not found at {self.dataset_path}")

    @function_printer("Updating cache")
    def update_cache(self) -> pl.DataFrame:
        """
        Update the cached dataset with the latest data from PocketBase.

        Returns
        -------
        pl.DataFrame
            The updated dataset as a Polars DataFrame.
        """
        data: pl.DataFrame = self.get_dataset()
        self._cache_dataset(data)
        return data

    def _cache_dataset(self, data: pl.DataFrame) -> None:
        """
        Save the dataset to the cache directory.

        Parameters
        ----------
        data : pl.DataFrame
            The dataset to cache.
        """
        data.write_csv(self.dataset_path)
        print(f"Dataset cached at {self.dataset_path}")
        return None

    def _parse_records(self, records: list[Record]) -> list[dict]:
        """
        Parse a list of PocketBase records into dictionaries.

        Parameters
        ----------
        records : list of Record
            List of PocketBase records.

        Returns
        -------
        list of dict
            Parsed dataset as a list of dictionaries.
        """
        return [self._parse_record(record) for record in records]

    @staticmethod
    def _parse_record(record: Record) -> dict:
        """
        Remove boilerplate keys from a PocketBase record.

        Parameters
        ----------
        record : Record
            The PocketBase record to parse.

        Returns
        -------
        dict
            A dictionary representation of the record without boilerplate keys.
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
