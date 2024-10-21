from lamindb_setup.core._aws_credentials import get_aws_credentials_manager
from lamindb_setup.core.upath import UPath


def test_anon_piblic():
    aws_credentials_manager = get_aws_credentials_manager()
    assert aws_credentials_manager.anon_public is not None
    assert not aws_credentials_manager.anon_public

    aws_credentials_manager.anon_public = True

    path = aws_credentials_manager.enrich_path(UPath("s3://cellxgene-data-public"))
    assert path.storage_options["anon"]
