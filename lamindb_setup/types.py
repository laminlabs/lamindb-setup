"""Types.

.. autoclass:: AnyPathStr
.. autoclass:: StorageType
"""

from __future__ import annotations

# we need Union here because __future__ annotations doesn't work with TypeAlias
from pathlib import Path
from typing import Literal, Union

from upath import UPath

# Cloud UPath is not a subclass of Path anymore, local UPath is a subclass of Path
AnyPath = Union[Path, UPath]
AnyPathStr = Union[str, AnyPath]
StorageType = Literal["local", "s3", "gs", "hf", "http", "https"]
