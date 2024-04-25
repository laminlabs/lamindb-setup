from __future__ import annotations

from lamindb_setup.core.upath import UPath, compute_file_tree, get_stat_dir_cloud


def test_get_stat_dir_cloud_aws():
    string_path = "s3://lamindb-dev-datasets/iris_studies/study0_raw_images"
    path = UPath(string_path, anon=True)
    _, n_objects_file_tree = compute_file_tree(path)
    size, hash, hash_type, n_objects = get_stat_dir_cloud(path)
    assert n_objects == n_objects_file_tree
    assert hash == "wVYKPpEsmmrqSpAZIRXCFg"
    assert hash_type == "md5-d"
    assert size == 656692


def test_get_stat_dir_cloud_gcp():
    string_path = "gs://rxrx1-europe-west4/images/test/HEPG2-08"
    path = UPath(string_path, anon=True)
    size, hash, hash_type, n_objects = get_stat_dir_cloud(path)
    assert n_objects == 14772
    assert hash == "6r5Hkce0UTy7X6gLeaqzBA"
    assert hash_type == "md5-d"
    assert size == 994441606
