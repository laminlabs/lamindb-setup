import os

import lamindb_setup as ln_setup


def test_user_exists():
    # cannot test this on staging because of email rate limit right now
    if os.getenv("LAMIN_ENV") == "prod":
        # test user exists
        assert ln_setup.signup("testuser1@lamin.ai") == "user-exists"
