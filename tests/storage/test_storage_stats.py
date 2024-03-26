from lamindb_setup.core.upath import get_stat_dir_cloud, UPath, compute_file_tree


def test_get_stat_dir_cloud():
    string_path = "s3://lamindb-dev-datasets/iris_studies/study0_raw_images"
    path = UPath(string_path, anon=True)
    _, n_objects_file_tree = compute_file_tree(path)
    size, hash, hash_type, n_objects = get_stat_dir_cloud(
        UPath("s3://lamindb-dev-datasets/iris_studies/study0_raw_images", anon=True)
    )
    assert n_objects == n_objects_file_tree
    assert hash == "wVYKPpEsmmrqSpAZIRXCFg"
    assert hash_type == "md5-d"
    assert size == 656692
