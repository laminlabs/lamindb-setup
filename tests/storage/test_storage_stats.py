from lamindb_setup.core.upath import get_stat_dir_s3, UPath


def test_get_stat_dir_s3():
    string_path = "s3://lamindb-dev-datasets/iris_studies/study0_raw_images"
    path = UPath(string_path, anon=True)
    path.view_tree()
    _, _, _, n_objects = get_stat_dir_s3(
        UPath("s3://lamindb-dev-datasets/iris_studies/study0_raw_images", anon=True)
    )
    assert n_objects == path._n_objects
