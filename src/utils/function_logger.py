from pathlib import Path

from ..config import logger
from functools import wraps


def function_logger(LOGGER_DIR: Path = Path("logs"), level: str = "INFO"):
    """
    A decorator to log the execution of a function.

    Args:
        func (callable): The function to be decorated.

    Returns:
        callable: The wrapped function with logging.
    """

    assert isinstance(LOGGER_DIR, Path), "LOGGER_DIR must be a Path object."

    def decorator(func):
        assert callable(func), "The decorated object must be callable."

        @wraps(func)
        def wrapper(*args, **kwargs):
            logger.add(
                LOGGER_DIR / f"{func.__name__}.log", rotation="10 MB", level=level
            )
            logger.info(f"Executing {func.__name__}({args=}, {kwargs=})")

            try:
                result = func(*args, **kwargs)
            except Exception as e:
                logger.error(f"Error in {func.__name__}(): {e}")
                raise

            logger.success(f"{func.__name__}() executed successfully.")
            return result

        return wrapper

    return decorator
