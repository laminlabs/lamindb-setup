from pathlib import Path

import lndb


def test_move_storage_location():
    lndb.init(storage="mydata", _test=True)
    # assume we move the storage location
    Path("./mydata").rename("./mydata_new_loc")
    # with pytest.raises(
    #     RuntimeError
    # ):  # triggers because it does not find the SQLite file anymore
    #     lndb.load("mydata", _test=True)
    # now account for the new storage location
    lndb.load("mydata", storage="./mydata_new_loc", _test=True)
    assert (
        lndb.settings.instance.storage.root.as_posix()
        == Path("./mydata_new_loc").resolve().as_posix()
    )
    assert lndb.settings.instance.storage.cache_dir is None
    assert (
        lndb.settings.instance.db
        == f"sqlite:///{Path('./mydata_new_loc').resolve().as_posix()}/mydata.lndb"
    )
