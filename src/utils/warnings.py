import functools
import warnings
from . import check_type


def deprecated(reason: str = ""):
    """
    Mark a function or method as deprecated.

    This decorator issues a ``DeprecationWarning`` whenever the decorated
    function is called. It is useful for notifying users that a function is
    obsolete and may be removed in a future release.

    Parameters
    ----------
    reason : str, optional
        Additional information about the deprecation, such as guidance on
        alternative functions to use. Default is an empty string.

    Returns
    -------
    decorator : callable
        A decorator that wraps the input function, issuing a deprecation
        warning on each call.

    Warns
    -----
    DeprecationWarning
        If the decorated function is called.

    Examples
    --------
    >>> from utils.decorators import deprecated
    >>>
    >>> @deprecated("Use `new_function` instead.")
    ... def old_function(x, y):
    ...     return x + y
    >>>
    >>> old_function(1, 2)
    __main__:1: DeprecationWarning: old_function is deprecated. Use `new_function` instead.
    3
    """
    check_type(reason, str, "reason")

    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            warnings.warn(
                f"{func.__name__} is deprecated. {reason}",
                category=DeprecationWarning,
                stacklevel=2,
            )
            return func(*args, **kwargs)

        return wrapper

    return decorator
