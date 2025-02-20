from lamindb_setup.core._aws_options import get_aws_options_manager
from lamindb_setup.core.upath import UPath


def test_aws_options_manager():
    aws_options_manager = get_aws_options_manager()
    assert aws_options_manager.anon_public is not None
    assert not aws_options_manager.anon_public

    aws_options_manager.anon_public = True

    path = aws_options_manager.enrich_path(UPath("s3://cellxgene-data-public/folder"))
    assert path.storage_options["anon"]
    assert "s3://cellxgene-data-public/" in aws_options_manager._credentials_cache

    path = aws_options_manager.enrich_path(UPath("s3://lamin-eu-west-2/folder"))
    assert not path.storage_options["anon"]
    assert "s3://lamin-eu-west-2/folder/" in aws_options_manager._credentials_cache
