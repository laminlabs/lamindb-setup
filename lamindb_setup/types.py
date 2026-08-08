"""Types.

.. autoclass:: AnyPathStr
.. autoclass:: StorageType
"""

from __future__ import annotations

import importlib
import sys

# we need Union here because __future__ annotations doesn't work with TypeAlias
from pathlib import Path
from typing import TYPE_CHECKING, Literal, Union

if TYPE_CHECKING:
    from upath import UPath
elif "sphinx" in sys.modules:
    # Keep rich type rendering in Sphinx docs.
    UPath = importlib.import_module("upath").UPath

# Cloud UPath is not a subclass of Path anymore, local UPath is a subclass of Path
AnyPath = Union[Path, "UPath"]
AnyPathStr = Union[str, AnyPath]
StorageType = Literal["local", "s3", "gs", "hf", "http", "https"]
