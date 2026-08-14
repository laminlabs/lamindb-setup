from __future__ import annotations

from types import SimpleNamespace

import lamindb_setup as ln_setup
from lamindb_setup.core._settings import settings


def test_to_url():
    # us-east-1 / AWS Dev
    # public bucket
    assert (
        ln_setup.core.upath.create_path("s3://lamindata/test-folder").to_url()
        == "https://lamindata.s3.amazonaws.com/test-folder"
    )
    # private bucket
    assert (
        ln_setup.core.upath.create_path(
            "s3://lamindb-setup-private-bucket/test-folder"
        ).to_url()
        == "https://lamindb-setup-private-bucket.s3.amazonaws.com/test-folder"
    )
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
        == "https://lamin-eu-central-1.s3-eu-central-1.amazonaws.com/9fm7UN13/test-folder"
    )


def test_to_url_gcs_root():
    upath = ln_setup.core.upath.UPath(
        "gs://rxrx1-europe-west4/images/test/HEPG2-08/Plate1/B02_s1_w1.png"
    )
    assert (
        upath.to_url()
        == "https://storage.googleapis.com/rxrx1-europe-west4/images/test/HEPG2-08/Plate1/B02_s1_w1.png"
    )


def test_to_url_https_root():
    upath = ln_setup.core.upath.UPath("https://example.com/files/document.txt")
    assert upath.to_url() == "https://example.com/files/document.txt"


def test_to_url_s3_hub_private_route(monkeypatch):
    monkeypatch.setattr(ln_setup.core.upath, "_is_public_s3_path", lambda _: False)
    monkeypatch.setattr(
        settings,
        "_instance_settings",
        SimpleNamespace(is_on_hub=True, ui_url="https://app.lamin.ai"),
        raising=False,
    )
    upath = ln_setup.core.upath.UPath("s3://lamindb-ci/test-data/test.parquet")
    assert (
        upath.to_url()
        == "https://app.lamin.ai/storage/s3/lamindb-ci%2F/test-data/test.parquet"
    )


def test_to_url_s3_public_stays_native(monkeypatch):
    monkeypatch.setattr(ln_setup.core.upath, "_is_public_s3_path", lambda _: True)
    monkeypatch.setattr(ln_setup.core.upath, "get_storage_region", lambda _: "us-east-1")
    monkeypatch.setattr(
        settings,
        "_instance_settings",
        SimpleNamespace(is_on_hub=True, ui_url="https://app.lamin.ai"),
        raising=False,
    )
    upath = ln_setup.core.upath.UPath("s3://lamindb-ci/test-data/test.parquet")
    assert upath.to_url() == "https://lamindb-ci.s3.amazonaws.com/test-data/test.parquet"
