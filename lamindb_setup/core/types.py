"""Types.

.. autosummary::
   :toctree: .

   UPathStr
"""
from __future__ import annotations
from pathlib import Path

# UPath is subclass of Path, hence, it's not necessary to list UPath
# we keep it in the name of the TypeVar to make it clear to users that
# cloud paths are allowed / PathStr is often associated with local paths
UPathStr = str | Path  # typing.TypeAlias, >3.10 on but already deprecated
