import sys
import warnings
from importlib.metadata import entry_points


def call_registered_entry_points(group, **kwargs):
    """load and call entry points registered under group."""
    if sys.version_info >= (3, 10):
        eps = entry_points(group=group)
    else:
        eps = entry_points().get(group, [])

    for ep in eps:
        func = ep.load()
        try:
            func(**kwargs)
        except BaseException as e:
            warnings.warn(
                f"Error loading entry point of group {group!r}: {ep} -> {e}",
                RuntimeWarning,
                stacklevel=2,
            )
