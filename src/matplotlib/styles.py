from enum import Enum

from ..config import MPL_STYLE_DIR


class Styles(Enum):
    """Enum for Matplotlib styles used in the project."""

    CMR10 = MPL_STYLE_DIR / "iragca_cmr10.mplstyle"
    ML = MPL_STYLE_DIR / "iragca_ml.mplstyle"
    ML2 = MPL_STYLE_DIR / "iragca_ml2.mplstyle"
