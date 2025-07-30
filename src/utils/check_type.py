from typing import Any


def check_type(var: Any, expected_types: type, var_name: str) -> bool:
    """Check if a variable is of the expected type(s)."""

    if not isinstance(expected_types, list):
        expected_types = [expected_types]

    if not all(isinstance(t, type) for t in expected_types):
        raise TypeError("Expected types must be a list of type objects.")

    if type(var) not in expected_types:
        raise TypeError(
            f"Variable '{var_name}' must be of type(s) {expected_types}, got {type(var)} instead."
        )

    return True
