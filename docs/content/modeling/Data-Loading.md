# DatasetLoader

[`Source`](https://github.com/iragca/capstone-project-2/blob/master/src/data/datasetloader.py)

This is a utility class for loading, caching, and parsing datasets from a PocketBase warehouse.

```{card}
:class-header: bg-light
:class-card: border-0 shadow-none

Parameters
^^^
`api_key` (str)
:   The [RapidAPI](https://rapidapi.com/) token key used to authenticate API use.
```

```{card}
:class-header: bg-light
:class-card: border-0 shadow-none

Attributes
^^^
`cache_dir`  ([*Path*](https://docs.python.org/3/library/pathlib.html#pathlib.Path))
:  Directory where the dataset cache is stored. Default: `src.config.DATA_DIR.value / ".cache"`

`dataset_path`  ([*Path*](https://docs.python.org/3/library/pathlib.html#pathlib.Path))
:  Path to the cached dataset CSV file. Default: `self.cache_dir / "dataset_cache.csv"`

`pb` (PBWarehouse)
: pb (PBWarehouse): An instance of PBWarehouse for fetching data from PocketBase.
```

Attributes:
pb (PBWarehouse): An instance of PBWarehouse for fetching data from PocketBase.

Methods:
load_dataset(cache=True, dtype=pl.DataFrame | nx.Graph | nx.DiGraph) -> pl.DataFrame | nx.Graph | nx.DiGraph:
Loads the dataset from cache or PocketBase, optionally caching it, and returns it as a DataFrame or graph.
get_dataset() -> pl.DataFrame:
Fetches the dataset from PocketBase and returns it as a Polars DataFrame.
get_cached_dataset() -> pl.DataFrame:
Loads the cached dataset from disk and returns it as a Polars DataFrame.
update_cache() -> pl.DataFrame:
Fetches the latest dataset from PocketBase and updates the cache.
\_cache_dataset(data: pl.DataFrame) -> None:
Saves the provided DataFrame to the cache directory.
\_parse_records(records: list[Record]) -> list[dict]:
Parses a list of PocketBase records into a list of dictionaries, removing boilerplate keys.
\_parse_record(record: Record) -> dict:
Static method to parse a single PocketBase record into a dictionary, removing boilerplate keys.
Raises:
ValueError: If an unsupported dtype is provided to load_dataset.
FileNotFoundError: If the dataset or cache is not found and cannot be loaded.
