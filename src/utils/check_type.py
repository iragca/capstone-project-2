from typing import Any

def check_type(var: Any, expected_type: type, var_name: str) -> bool:
    """Check if a variable is of the expected type."""
    if not isinstance(var, expected_type):
        raise TypeError(
            f"Expected '{var_name}' of type {expected_type.__name__}, got {type(var).__name__}."
        )

    return True
