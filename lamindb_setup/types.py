"""Types.

.. autoclass:: UPathStr
.. autoclass:: StorageType
"""

from __future__ import annotations

# we need Union here because __future__ annotations doesn't work with TypeAlias
from pathlib import Path
from typing import Literal, Union

from upath import UPath

# UPath is not a subclass of Path anymore
AnyPath = Union[Path, UPath]
AnyPathStr = Union[str, AnyPath]
UPathStr = AnyPathStr
StorageType = Literal["local", "s3", "gs", "hf", "http", "https"]
