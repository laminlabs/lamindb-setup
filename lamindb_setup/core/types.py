import warnings

warnings.warn(
    "`lamindb_setup.core.types` is deprecated, use `lamindb_setup.types` instead.",
    DeprecationWarning,
    stacklevel=2,
)

from lamindb_setup.types import AnyPathStr  # backward compatibility
