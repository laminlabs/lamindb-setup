import os

from lamindb_setup import login, settings


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
