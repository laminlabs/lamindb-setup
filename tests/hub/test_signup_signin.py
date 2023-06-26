import secrets
import string

from lamindb_setup.dev._hub_core import sign_in_hub


def base26(n_char: int):
    alphabet = string.ascii_lowercase
    return "".join(secrets.choice(alphabet) for i in range(n_char))


def test_signin_hub(auth_2):
    response = sign_in_hub(auth_2["email"], auth_2["password"])
    assert response == "complete-signup"
