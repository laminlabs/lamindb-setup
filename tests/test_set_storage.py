import lamindb as ln


def test_set_storage_sqlite():
    ln.setup.delete("mydata")
    ln.setup.init(storage="./mydata", _test=True)
    assert ln.setup.set.storage("mydata2") == "set-storage-failed"
    ln.setup.delete("mydata")
