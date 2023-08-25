import pytest

import lamindb_setup as ln_setup


def test_delete_invalid_name():
    with pytest.raises(ValueError) as error:
        ln_setup.delete("invalid/name")
    assert (
        error.exconly()
        == "ValueError: Invalid instance name: '/' delimiter not allowed."
    )
