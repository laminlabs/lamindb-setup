from lamindb_setup.core.upath import get_stat_dir_s3, UPath


def test_get_stat_dir_s3():
    import re

    message = UPath(
        "s3://lamindb-dev-datasets/iris_studies/study0_raw_images"
    ).view_tree()
    num_objects_view_tree = re.search(r"(\d+) files", message).group(1)
    _, _, _, num_objects_stat = get_stat_dir_s3(
        UPath("s3://lamindb-dev-datasets/iris_studies/study0_raw_images")
    )
    assert num_objects_stat == int(num_objects_view_tree)
