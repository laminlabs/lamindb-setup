import lamindb_setup as ln_setup


def test_user_exists():
    assert ln_setup.signup("testuser1@lamin.ai") == "user-exists"
