from __future__ import annotations

import lamindb_setup as ln_setup
import pytest
from lamindb_setup._connect_instance import InstanceNotFoundError


def test_delete_invalid_name():
    with pytest.raises(InstanceNotFoundError):
        ln_setup.delete("invalid/name")
