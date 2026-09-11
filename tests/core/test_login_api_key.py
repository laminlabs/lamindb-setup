from __future__ import annotations

from unittest.mock import call, patch

import lamindb_setup as ln_setup
import pytest
from lamindb_setup.errors import ApiKeyExpired
from supabase_functions.errors import FunctionsHttpError


def test_login_expired_api_key():
    with (
        patch(
            "lamindb_setup.core._hub_core.call_with_fallback",
            side_effect=FunctionsHttpError("API key has expired."),
        ),
        patch("lamindb_setup.core._hub_core.logger.error") as error,
        pytest.raises(ApiKeyExpired) as exc_info,
    ):
        ln_setup.login(api_key="expired-key")
    assert exc_info.value.__cause__ is None
    assert error.call_args_list == [
        call("Could not login."),
        call("Your API key is expired."),
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
