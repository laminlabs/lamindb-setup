from lamindb_setup.core.upath import get_stat_dir_s3, UPath, compute_file_tree


def test_get_stat_dir_s3():
    string_path = "s3://lamindb-dev-datasets/iris_studies/study0_raw_images"
    path = UPath(string_path, anon=True)
    _, n_objects_file_tree = compute_file_tree(path)
    _, _, _, n_objects = get_stat_dir_s3(
        UPath("s3://lamindb-dev-datasets/iris_studies/study0_raw_images", anon=True)
    )
    assert n_objects == n_objects_file_tree
