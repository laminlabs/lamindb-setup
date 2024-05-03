import lamindb_setup as ln_setup
import pytest
from lamindb_setup._connect_instance import InstanceNotFoundError
from lamindb_setup._set_managed_storage import set_managed_storage


def test_set_storage_sqlite():
    try:
        ln_setup.delete("mydata", force=True)
    except InstanceNotFoundError:
        pass
    ln_setup.init(storage="./mydata", _test=True)
    with pytest.raises(ValueError):
        set_managed_storage("mydata2")
    ln_setup.delete("mydata", force=True)
