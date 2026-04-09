from __future__ import annotations

import lamindb_setup.core._hub_client as hub_client
import pytest
from httpx import ConnectError
from lamindb_setup.core._hub_client import PROD_URL, connect_hub, connect_hub_with_auth
from lamindb_setup.core._hub_core import connect_instance_hub


def test_switch_env(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("LAMIN_ENV", "prod")
    prod_client = connect_hub()
    assert prod_client.supabase_url == PROD_URL

    monkeypatch.setenv("LAMIN_ENV", "staging")
    staging_client = connect_hub()
    assert staging_client.supabase_url == "https://amvrvdwndlqdzgedrqdv.supabase.co"

    monkeypatch.setenv("LAMIN_ENV", "prod")
    prod_client_again = connect_hub()
    assert prod_client_again.supabase_url == PROD_URL


def test_connect_instance_fallbacks():
    hub_client.PROD_URL = (  # deactivated prod url
        "https://inactive.lamin.ai"
    )
    with pytest.raises(ConnectError):
        connect_hub_with_auth(renew_token=True)
    # should not error due to fallback
    connect_instance_hub(
        owner="laminlabs",
        name="lamindata",
    )
    hub_client.PROD_URL = PROD_URL
