import os
import pytest
import lamindb_setup as ln_setup
from gotrue.errors import AuthUnknownError
from lamindb_setup import login, settings
from lamindb_setup.dev._hub_core import (
    load_instance,
    sign_in_hub,
)


def test_switch_env():
    # testuser1 is defined in prod & staging with the same password
    assert os.getenv("LAMIN_ENV") == "prod"

    # check whether we can log in
    login("static-testuser1@lamin.ai", password="static-testuser1-password")
    assert settings.user.email == "static-testuser1@lamin.ai"

    # testuser1.staging is defined only in staging
    os.environ["LAMIN_ENV"] = "staging"

    login("testuser1.staging@lamin.ai", password="password")
    assert settings.user.email == "testuser1.staging@lamin.ai"

    # back to prod
    os.environ["LAMIN_ENV"] = "prod"


def test_load_instance_fallbacks():
    prod_url = ln_setup.dev._hub_client.PROD_URL
    ln_setup.dev._hub_client.PROD_URL = (  # deactivated prod url
        "https://inactive.lamin.ai"
    )
    with pytest.raises(AuthUnknownError):
        ln_setup.dev._hub_client.connect_hub_with_auth(renew_token=True)
    assert not isinstance(
        sign_in_hub(
            email="static-testuser1@lamin.ai", password="static-testuser1-password"
        ),
        str,
    )
    load_instance(
        owner="static-testuser1",
        name="static-testinstance1",
    )
    ln_setup.dev._hub_client.PROD_URL = prod_url
