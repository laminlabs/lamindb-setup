from lamindb_setup._setup_user import signup
from lamindb_setup.dev._hub_core import sign_in_hub


def test_signin_hub(auth_2):
    response = sign_in_hub(auth_2["email"], auth_2["password"])
    assert response == "complete-signup"


# compare with test in test_signup.py in two-envs
# where we test this on prod
def test_signup():
    assert signup("testuser1@lamin.ai") is None
