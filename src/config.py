import os
import pathlib
from enum import Enum
from pathlib import Path

import pyotp
from dotenv import load_dotenv
from loguru import logger

from src.utils import check_env_variable

PROJECT_ROOT: Path = pathlib.Path(__file__).resolve().parent.parent
ENV_FILE: Path = PROJECT_ROOT / ".env"
DATA_DIR: Path = PROJECT_ROOT / "data"
EXTERNAL_DATA_DIR: Path = DATA_DIR / "external"
INTERIM_DATA_DIR: Path = DATA_DIR / "interim"

if not ENV_FILE.exists():
    logger.warning(
        f".env file not found at {ENV_FILE}. Please create one with the required environment variables."
    )
else:
    load_dotenv(dotenv_path=ENV_FILE)
    logger.info(f"Loaded environment variables from {ENV_FILE}")

required_env_vars: list[str] = []
non_essential_env_vars: list[str] = []

for var in required_env_vars:
    value: str | None = os.getenv(var)
    check_env_variable(value, var, important=True)

for var in non_essential_env_vars:
    value = os.getenv(var)
    check_env_variable(value, var)


class Settings(Enum):
    """Settings class to hold environment variables."""

    PYOTP = pyotp.TOTP(os.getenv("X_TOTP", ""))

    # Add more environment variables as needed
    # EXAMPLE = os.getenv("example")
    X_USERNAME = os.getenv("X_USERNAME", "")
    X_PASSWORD = os.getenv("X_PASSWORD", "")
    X_TOTP = PYOTP.now()
    X_RAPIDAPI_KEY = os.getenv("X_RAPIDAPI_KEY", "")


# Log key paths
logger.info(f"PROJECT_ROOT: {PROJECT_ROOT}")
logger.info(f"DATA_DIR: {DATA_DIR}")
