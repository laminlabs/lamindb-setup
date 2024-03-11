import lamindb_setup as ln_setup
from lamindb_setup._add_remote_storage import switch_default_storage


def test_set_storage_sqlite():
    ln_setup.delete("mydata", force=True)
    ln_setup.init(storage="./mydata", _test=True)
    assert switch_default_storage("mydata2") == "set-storage-failed"
    ln_setup.delete("mydata", force=True)
