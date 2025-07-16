from pathlib import Path


def ensure_path(path: Path) -> None:
    """
    Ensure that the given path is a Path object.

    Args:
        path (str): The path to be converted.

    Returns:
        Path: A Path object representing the given path.
    """
    assert isinstance(path, Path), f"Expected Path type, got {type(path)}"

    if not path.exists():
        path.mkdir(parents=True, exist_ok=True)
        print(f"Created directory: {path}")

    return path
