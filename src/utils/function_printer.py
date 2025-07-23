from .inline_print import inline_print
from functools import wraps


def function_printer(message: str):
    """A decorator to print a message before and after the execution of a function.

    Args:
        message (str): The message to print.

    Returns:
        callable: The wrapped function with inline printing.
    """

    def decorator(func):
        assert callable(func), "The decorated object must be callable."

        @wraps(func)
        def wrapper(*args, **kwargs):
            try:
                inline_print(f"{message}...")
                result = func(*args, **kwargs)
                inline_print(f"✅ {message} - Done\n.")
                return result
            except Exception as e:
                inline_print(f"❌ {message} failed: [{type(e).__name__}] {e}\n")
                raise

        return wrapper

    return decorator
