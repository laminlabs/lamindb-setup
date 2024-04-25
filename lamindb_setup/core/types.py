from __future__ import annotations

"""Types.

.. autosummary::
   :toctree: .

   UPathStr
"""
# we need Union here because __future__ annotations doesn't work with TypeAlias
from pathlib import Path
from typing import (
    Union,
)

# UPath is subclass of Path, hence, it's not necessary to list UPath
# we keep it in the name of the TypeAlias to make it clear to users that
# cloud paths are allowed / PathStr is often associated with local paths
UPathStr = Union[str, Path]  # typing.TypeAlias, >3.10 on but already deprecated
