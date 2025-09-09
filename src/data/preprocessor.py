import re

import polars as pl
from sklearn.preprocessing import LabelEncoder

from ..utils import function_printer


class Preprocessor:
    """
    Preprocess data stored in a Polars DataFrame.

    This class provides methods to clean and transform tabular data, including:
    - Removing HTML anchor tags from text.
    - Categorizing and encoding the 'source' column.
    - Applying custom preprocessing steps.

    Parameters
    ----------
    data : pl.DataFrame
        The input dataset to be preprocessed.

    Attributes
    ----------
    data : pl.DataFrame
        The preprocessed dataset.

    Methods
    -------
    preprocess(categorize_source=True) -> pl.DataFrame
        Preprocesses the dataset, optionally categorizing and encoding the 'source' column.
    categorize_source_column() -> None
        Cleans and encodes the 'source' column by removing anchor tags, categorizing source strings,
        and label encoding the results.
    remove_anchor_tags(text) -> str
        Remove HTML anchor tags from a string.
    categorize_source_str(text) -> str
        Categorize a source string into 'anonymized', 'unknown', or the lowercased string.
    """

    def __init__(self, data: pl.DataFrame):
        """
        Initialize the Preprocessor with a Polars DataFrame.

        Parameters
        ----------
        data : pl.DataFrame
            Input dataset to preprocess.
        """
        self.data = data

    @function_printer("Preprocessing data")
    def preprocess(self, categorize_source: bool = True) -> pl.DataFrame:
        """
        Preprocess the dataset.

        This method applies various preprocessing steps to the dataset. Currently, it optionally
        categorizes and encodes the 'source' column.

        Parameters
        ----------
        categorize_source : bool, optional
            Whether to apply the 'source' column categorization and encoding (default is True).

        Returns
        -------
        pl.DataFrame
            The preprocessed dataset.
        """

        if categorize_source:
            self.categorize_source_column()

        return self.data

    @function_printer("Categorizing source column")
    def categorize_source_column(self) -> None:
        """
        Clean and encode the 'source' column.

        Steps performed:
        1. Remove HTML anchor tags from all string entries.
        2. Categorize each string: empty → 'unknown', erased → 'anonymized', otherwise lowercased.
        3. Encode the resulting strings into integer labels using sklearn's LabelEncoder.

        Returns
        -------
        None
        """
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
        """
        Remove HTML anchor tags from a string.

        Parameters
        ----------
        text : str
            Input string potentially containing HTML anchor tags.

        Returns
        -------
        str
            The input string with all anchor tags removed.
        """
        return re.sub(r"<a[^>]*>(.*?)</a>", r"\1", text)

    @staticmethod
    def categorize_source_str(text: str) -> str:
        """
        Categorize a source string.

        Rules:
        - Empty string → 'unknown'
        - Strings starting with 'erased' → 'anonymized'
        - Otherwise, return the lowercase version of the string.

        Parameters
        ----------
        text : str
            Source string to categorize.

        Returns
        -------
        str
            Categorized source string.
        """
        if len(text) == 0:
            return "unknown"

        if re.search(r"^erased.*", text):
            return "anonymized"

        return text.lower()
