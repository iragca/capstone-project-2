import re

import polars as pl
from sklearn.preprocessing import LabelEncoder

from ..utils import function_printer


class Preprocessor:
    """
    A class to preprocess text data for machine learning tasks.
    """

    def __init__(self, data: pl.DataFrame):
        self.data = data

    @function_printer("Preprocessing data")
    def preprocess(self, categorize_source: bool = True) -> pl.DataFrame:
        """Preprocess the data."""

        if categorize_source:
            self.categorize_source_column()

        return self.data

    @function_printer("Categorizing source column")
    def categorize_source_column(self) -> None:
        """Remove anchor tags from a specific column."""
        self.data = self.data.with_columns(
            pl.col("source").map_elements(
                lambda x: self.categorize_source_str(self.remove_anchor_tags(x))
                if isinstance(x, str)
                else x,
                return_dtype=pl.Utf8,
            )
        )

        label_encoder = LabelEncoder()
        label_encoder.fit(self.data["source"])
        self.data = self.data.with_columns(
            pl.col("source").map_elements(
                lambda x: label_encoder.fit_transform([x])[0]
                if isinstance(x, str)
                else x,
                return_dtype=pl.Int64,
            )
        )

    @staticmethod
    def remove_anchor_tags(text: str) -> str:
        """Remove anchor tags from the text."""
        return re.sub(r"<a[^>]*>(.*?)</a>", r"\1", text)

    @staticmethod
    def categorize_source_str(text: str) -> str:
        if len(text) == 0:
            return "unknown"

        if re.search(r"^erased.*", text):
            return "anonymized"

        return text.lower()
