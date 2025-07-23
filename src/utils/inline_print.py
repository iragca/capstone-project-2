import sys


def inline_print(message: str) -> None:
    """Print a message inline."""
    sys.stdout.write(f"\r{message}")
    sys.stdout.flush()
    return None
