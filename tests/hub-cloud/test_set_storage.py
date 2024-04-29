import lamindb_setup as ln_setup
import pytest
from lamindb_setup._add_remote_storage import add_managed_storage


def test_set_storage_sqlite():
    ln_setup.delete("mydata", force=True)
    ln_setup.init(storage="./mydata", _test=True)
    with pytest.raises(ValueError):
        add_managed_storage("mydata2")
    ln_setup.delete("mydata", force=True)
