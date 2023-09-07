import os

from lamindb_setup.dev._hub_client import Environment, load_connector


def test_runs_locally():
    assert os.environ["LAMIN_ENV"] == "local"
    assert load_connector().url != Environment().supabase_api_url
