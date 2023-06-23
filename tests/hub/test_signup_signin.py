import secrets
import string

from lamindb_setup.dev._hub_core import sign_in_hub, sign_up_hub


def base26(n_char: int):
    alphabet = string.ascii_lowercase
    return "".join(secrets.choice(alphabet) for i in range(n_char))


def test_signup_hub():
    email = f"lamin.ci.user.{base26(6)}@gmail.com"
    password = sign_up_hub(email)
    assert len(password) == 40


def test_signin_hub(auth_2):
    response = sign_in_hub(auth_2["email"], auth_2["password"])
    id, handle, _, _ = response
    assert id == auth_2["id"]
    assert handle == auth_2["handle"]
