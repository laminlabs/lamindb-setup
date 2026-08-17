from __future__ import annotations

from types import SimpleNamespace

import lamindb_setup as ln_setup
import pytest
from lamindb_setup.core._settings import settings


def test_to_url_s3_public_us_east_1(monkeypatch):
    monkeypatch.setattr(
        ln_setup.core.upath, "_is_publicly_accessible_path", lambda _: True
    )
    monkeypatch.setattr(
        ln_setup.core.upath, "get_storage_region", lambda _: "us-east-1"
    )
    upath = ln_setup.core.upath.UPath("s3://lamindata/test-folder")
    assert upath.to_url() == "https://lamindata.s3.amazonaws.com/test-folder"


def test_to_url_s3_public_regional(monkeypatch):
    monkeypatch.setattr(
        ln_setup.core.upath, "_is_publicly_accessible_path", lambda _: True
    )
    monkeypatch.setattr(
        ln_setup.core.upath, "get_storage_region", lambda _: "eu-central-1"
    )
    upath = ln_setup.core.upath.UPath("s3://lamindata-eu/test-folder")
    assert (
        upath.to_url()
        == "https://lamindata-eu.s3-eu-central-1.amazonaws.com/test-folder"
    )


def test_to_url_gcs_root(monkeypatch):
    monkeypatch.setattr(
        ln_setup.core.upath, "_is_publicly_accessible_path", lambda _: True
    )
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
    monkeypatch.setattr(
        ln_setup.core.upath, "_is_publicly_accessible_path", lambda _: False
    )
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
    monkeypatch.setattr(
        ln_setup.core.upath, "_is_publicly_accessible_path", lambda _: True
    )
    monkeypatch.setattr(
        ln_setup.core.upath, "get_storage_region", lambda _: "us-east-1"
    )
    monkeypatch.setattr(
        settings,
        "_instance_settings",
        SimpleNamespace(is_on_hub=True, ui_url="https://app.lamin.ai"),
        raising=False,
    )
    upath = ln_setup.core.upath.UPath("s3://lamindb-ci/test-data/test.parquet")
    assert (
        upath.to_url() == "https://lamindb-ci.s3.amazonaws.com/test-data/test.parquet"
    )


def test_to_url_s3_private_not_hub_raises(monkeypatch):
    monkeypatch.setattr(
        ln_setup.core.upath, "_is_publicly_accessible_path", lambda _: False
    )
    monkeypatch.setattr(
        settings,
        "_instance_settings",
        SimpleNamespace(is_on_hub=False, ui_url=None),
        raising=False,
    )
    upath = ln_setup.core.upath.UPath("s3://private-bucket/secret/file.csv")
    with pytest.raises(
        ValueError,
        match="must be publicly accessible or the artifact must be hosted on LaminHub",
    ):
        upath.to_url()


def test_to_url_gcs_private_raises(monkeypatch):
    monkeypatch.setattr(
        ln_setup.core.upath, "_is_publicly_accessible_path", lambda _: False
    )
    upath = ln_setup.core.upath.UPath(
        "gs://rxrx1-europe-west4/images/test/HEPG2-08/Plate1/B02_s1_w1.png"
    )
    with pytest.raises(ValueError, match="only supports publicly accessible GCS paths"):
        upath.to_url()
