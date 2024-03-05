from lamindb_setup.core.upath import UPath, create_path


def test_create_path():
    upath = UPath("s3://lamindb-ci/xyz/", default_fill_cache=False)
    assert "default_fill_cache" in upath._kwargs

    upath = create_path(upath)
    # test option inheritance
    assert "default_fill_cache" in upath._kwargs
    # test cache_regions setting for s3
    assert "cache_regions" in upath._kwargs
    # test removal of training slash
    assert upath.as_posix()[-1] != "/"
    assert (
        UPath("s3://lamindb-ci/xyz").as_posix()
        == create_path("s3://lamindb-ci/xyz/").as_posix()
    )
