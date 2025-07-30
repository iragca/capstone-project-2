from typing import Any

def check_type(var: Any, expected_type: type) -> bool:
    """Check if a variable is of the expected type."""
    if not isinstance(var, expected_type):
        raise TypeError(
            f"Expected variable of type {expected_type.__name__}, got {type(var).__name__}."
        )

    return True
