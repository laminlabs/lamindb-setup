import os

from lamindb_setup.dev._hub_client import Settings, lamindb_client_config_settings


def test_local():
    assert os.environ["LAMIN_ENV"] == "local"
    prod_config = lamindb_client_config_settings(Settings())
    assert prod_config["supabase_api_url"] == Settings().supabase_api_url
