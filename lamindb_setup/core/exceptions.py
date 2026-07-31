import warnings

warnings.warn(
    "`lamindb_setup.core.exceptions` is deprecated, use `lamindb_setup.errors` instead.",
    DeprecationWarning,
    stacklevel=2,
)

from lamindb_setup.errors import DefaultMessageException  # backwards compatibility
