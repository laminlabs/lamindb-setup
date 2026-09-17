from __future__ import annotations

from unittest.mock import call, patch

import lamindb_setup as ln_setup
import pytest
from lamindb_setup.errors import ApiKeyExpired, ApiKeyNotFound
from supabase_functions.errors import FunctionsHttpError


@pytest.mark.parametrize(
    ("hub_message", "error_type", "log_message"),
    [
        ("API key has expired.", ApiKeyExpired, "Your API key is expired."),
        ("API key not found.", ApiKeyNotFound, "Your API key was not found."),
    ],
)
def test_login_api_key_error(hub_message, error_type, log_message):
    with (
        patch(
            "lamindb_setup.core._hub_core.call_with_fallback",
            side_effect=FunctionsHttpError(hub_message),
        ),
        patch("lamindb_setup.core._hub_core.logger.error") as error,
        pytest.raises(error_type) as exc_info,
    ):
        ln_setup.login(api_key="bad-key")
    assert exc_info.value.__cause__ is None
    assert error.call_args_list == [
        call("Could not login."),
        call(log_message),
    ]


def test_login_invalid_api_key():
    invalid = FunctionsHttpError("Invalid API key.")
    with (
        patch(
            "lamindb_setup.core._hub_core.call_with_fallback",
            side_effect=invalid,
        ),
        patch("lamindb_setup.core._hub_core.logger.error") as error,
        pytest.raises(FunctionsHttpError) as exc_info,
    ):
        ln_setup.login(api_key="invalid-key")
    assert exc_info.value is invalid
    assert error.call_args_list == [
        call("Could not login."),
        call("Probably your API key is wrong."),
    ]
