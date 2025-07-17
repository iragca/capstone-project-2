from enum import Enum

import matplotlib.colors as mcolors

from ..config import logger


class Color(Enum):
    """Enum for colors used in the project.

    Primary colors are inspired by the accessible color palette from
    https://github.com/mpetroff/accessible-color-cycles
    """

    # Main
    BLUE = "#5790fc"
    ORANGE = "#f89c20"
    RED = "#e42536"
    PURPLE = "#964a8b"
    GRAY = "#9c9ca1"
    VIOLET = "#7a21dd"
    WHITE = "#ffffff"

    # Accents
    DARK_GRAY = "#3b3b3b"
    LIGHT_GRAY = "#BFBFBF"
    PUSH_TO_BG = "#dddddd"

    DARK_BLUE = "#072ea5"
    LIGHT_BLUE = "#91c2ed"

    DARK_ORANGE = "#ff3e06"
    LIGHT_ORANGE = "#fbb181"

    DARK_RED = "#a50000"
    LIGHT_RED = "#ea6466"

    SOFT_BLACK = "#333333"

    @classmethod
    def get_main_colors(cls):
        """Get primary colors as a list."""
        return [
            cls.BLUE.value,
            cls.ORANGE.value,
            cls.RED.value,
            cls.PURPLE.value,
            cls.GRAY.value,
            cls.VIOLET.value,
        ]

    @classmethod
    def get_accent_colors(cls):
        """Get secondary colors as a list."""
        return [
            cls.DARK_GRAY.value,
            cls.LIGHT_GRAY.value,
            cls.DARK_BLUE.value,
            cls.LIGHT_BLUE.value,
            cls.DARK_ORANGE.value,
            cls.LIGHT_ORANGE.value,
            cls.DARK_RED.value,
            cls.LIGHT_RED.value,
            cls.SOFT_BLACK.value,
        ]

    @classmethod
    def get_all_colors(cls: type) -> list:
        """Get all colors as a list."""
        pallete = cls.get_primary_colors() + cls.get_secondary_colors()
        return pallete

    @classmethod
    def BlWhOr(cls: type) -> mcolors.LinearSegmentedColormap:
        """Get a color palette for B/W."""
        pallete: list = [
            cls.BLUE.value,
            cls.WHITE.value,
            cls.ORANGE.value,
        ]
        return mcolors.LinearSegmentedColormap.from_list("BlWhOr", pallete, N=256)

    @classmethod
    def BlWhRd(cls: type) -> mcolors.LinearSegmentedColormap:
        """Get a color palette for B/W."""
        pallete: list = [
            cls.BLUE.value,
            cls.WHITE.value,
            cls.RED.value,
        ]
        return mcolors.LinearSegmentedColormap.from_list("BlWhRd", pallete, N=256)

    @classmethod
    def WhBl(cls: type) -> mcolors.LinearSegmentedColormap:
        """Get a color palette for B/W."""
        palette: list = [
            cls.WHITE.value,
            cls.BLUE.value,
        ]
        return mcolors.LinearSegmentedColormap.from_list("BlWhOr", palette, N=256)


logger.info(
    "Accessible color palette inspired by https://github.com/mpetroff/accessible-color-cycles."
)
