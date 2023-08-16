import lamindb_setup as ln_setup


def test_set_storage_sqlite():
    ln_setup.delete("mydata", force=True)
    ln_setup.init(storage="./mydata", _test=True)
    assert ln_setup.set.storage("mydata2") == "set-storage-failed"
    ln_setup.delete("mydata", force=True)
