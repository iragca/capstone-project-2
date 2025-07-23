import re

import polars as pl

from ..utils import function_printer


class Preprocessor:
    """
    A class to preprocess text data for machine learning tasks.
    """

    def __init__(self, data: pl.DataFrame):
        self.data = data

    @function_printer("Preprocessing data")
    def preprocess(self, remove_anchor_tags: bool = True) -> pl.DataFrame:
        """Preprocess the data."""

        if remove_anchor_tags:
            self.remove_anchor_tags_from_column("source")

        return self.data

    @function_printer("Removing anchor tags")
    def remove_anchor_tags_from_column(self, column: str) -> None:
        """Remove anchor tags from a specific column."""
        self.data = self.data.with_columns(
            pl.col(column).map_elements(
                lambda x: self.remove_anchor_tags(x) if isinstance(x, str) else x,
                return_dtype=pl.Utf8,
            )
        )

    @staticmethod
    def remove_anchor_tags(text: str) -> str:
        """Remove anchor tags from the text."""
        return re.sub(r"<a[^>]*>(.*?)</a>", r"\1", text)
