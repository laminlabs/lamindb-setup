import lamindb_setup as ln_setup


def test_to_url():
    # us-east-1 / AWS Dev
    # public bucket
    assert (
        ln_setup.core.UPath("s3://lamindata/test-folder").to_url()
        == "https://lamindata.s3.amazonaws.com/test-folder"
    )
    # private bucket
    assert (
        ln_setup.core.UPath("s3://lamindb-setup-private-bucket/test-folder")
        == "https://lamindb-setup-private-bucket.s3.amazonaws.com/test-folder"
    )
    # eu-central-1 / AWS Dev
    assert (
        ln_setup.core.UPath("s3://lamindata/test-folder").to_url()
        == "https://lamindata-eu.s3-eu-central-1.amazonaws.com/test-folder"
    )
    # eu-central-1 / AWS Hosted
    assert (
        ln_setup.core.UPath("s3://lamin-eu-central-1/test-folder").to_url()
        == "https://lamin-eu-central-1.s3-lamin-eu-central-1.amazonaws.com/test-folder"
    )
