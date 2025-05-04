from __future__ import annotations

from lamindb_setup.core._aws_options import HOSTED_REGIONS
from lamindb_setup.core._settings_storage import get_storage_region
from lamindb_setup.core.upath import (
    UPath,
    compute_file_tree,
    get_stat_dir_cloud,
    get_stat_file_cloud,
)


def test_get_stat_file_cloud_aws():
    string_path = "s3://bionty-assets/df_all__ncbitaxon__2023-06-20__Organism.parquet"
    path = UPath(string_path, anon=True)
    size, hash, hash_type = get_stat_file_cloud(path.stat().as_info())
    assert hash == "zQxieeCkNGNJWhPl3OfM8A"
    assert hash_type == "md5-5"
    assert size == 78148228


def test_get_stat_file_cloud_gcp():
    string_path = "gs://rxrx1-europe-west4/images/test/HEPG2-08/Plate1/B02_s1_w1.png"
    path = UPath(string_path)
    size, hash, hash_type = get_stat_file_cloud(path.stat().as_info())
    assert hash == "foEgLjmuUHO62CazxN97rA"
    assert hash_type == "md5"
    assert size == 65122


def test_get_stat_file_cloud_hf():
    string_path = "hf://datasets/Koncopd/lamindb-test/anndata/pbmc68k_test.h5ad"
    path = UPath(string_path)
    size, hash, hash_type = get_stat_file_cloud(path.stat().as_info())
    assert hash == "zY4nvir5FtTHY2f0GKn9Ac"
    assert hash_type == "sha1"
    assert size == 267036


def test_get_stat_file_cloud_http():
    string_path = "https://raw.githubusercontent.com/laminlabs/lamindb-setup/refs/heads/main/README.md"
    path = UPath(string_path)
    size, hash, hash_type = get_stat_file_cloud(path.stat().as_info())
    assert hash == "h3XDCTkMambmtSVpTQetLQ"
    assert hash_type == "md5-etag"
    assert size == 272


def test_get_stat_dir_cloud_aws():
    string_path = "s3://lamindata/iris_studies/study0_raw_images"
    path = UPath(string_path, anon=True)
    _, n_files_file_tree = compute_file_tree(path)
    size, hash, hash_type, n_files = get_stat_dir_cloud(path)
    assert n_files == n_files_file_tree
    assert hash == "IVKGMfNwi8zKvnpaD_gG7w"
    assert hash_type == "md5-d"
    assert size == 658465
    assert n_files == 51


def test_get_stat_dir_cloud_gcp():
    string_path = "gs://rxrx1-europe-west4/images/test/HEPG2-08"
    path = UPath(string_path)
    size, hash, hash_type, n_files = get_stat_dir_cloud(path)
    assert n_files == 14772
    assert hash == "6r5Hkce0UTy7X6gLeaqzBA"
    assert hash_type == "md5-d"
    assert size == 994441606


def test_get_stat_dir_cloud_hf():
    string_path = "hf://datasets/Koncopd/lamindb-test@main/sharded_parquet"
    path = UPath(string_path)
    size, hash, hash_type, n_files = get_stat_dir_cloud(path)
    assert n_files == 11
    assert hash == "oj6I3nNKj_eiX2I1q26qaw"
    assert hash_type == "md5-d"
    assert size == 42767


def test_get_storage_region():
    for region in HOSTED_REGIONS:
        assert get_storage_region(f"s3://lamin-{region}") == region
    assert get_storage_region(UPath("s3://lamindata", endpoint_url=None)) == "us-east-1"
    assert get_storage_region("s3://lamindata/?endpoint_url=") == "us-east-1"
