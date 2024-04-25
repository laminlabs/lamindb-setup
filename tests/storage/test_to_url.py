from __future__ import annotations

import lamindb_setup as ln_setup


def test_to_url():
    # us-east-1 / AWS Dev
    # public bucket
    assert (
        ln_setup.core.upath.create_path("s3://lamindata/test-folder").to_url()
        == "https://lamindata.s3.amazonaws.com/test-folder"
    )
    # private bucket
    # next PR
    # assert (
    #     ln_setup.core.upath.create_path("s3://lamindb-setup-private-bucket/test-folder")
    #     == "https://lamindb-setup-private-bucket.s3.amazonaws.com/test-folder"
    # )
    # eu-central-1 / AWS Dev
    assert (
        ln_setup.core.upath.create_path("s3://lamindata-eu/test-folder").to_url()
        == "https://lamindata-eu.s3-eu-central-1.amazonaws.com/test-folder"
    )
    # eu-central-1 / AWS Hosted
    # below is the default storage of the lamin-dev instance
    assert (
        ln_setup.core.upath.create_path(
            "s3://lamin-eu-central-1/9fm7UN13/test-folder"
        ).to_url()
        == "https://lamin-eu-central-1.s3-lamin-eu-central-1.amazonaws.com/9fm7UN13/test-folder"
    )
