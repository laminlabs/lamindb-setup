from __future__ import annotations

from datetime import datetime, timedelta, timezone
from unittest.mock import patch

import pytest
from lamindb_setup.core._hub_client import _warn_if_api_key_expiring

NOW = datetime(2026, 8, 25, 17, 23, 14, 401936, tzinfo=timezone.utc)


def _supabase_expires_at(days: int) -> str:
    dt = NOW + timedelta(days=days)
    return dt.strftime("%Y-%m-%d %H:%M:%S.%f") + "+00"


@pytest.mark.parametrize(
    ("expires_at", "expected"),
    [
        (_supabase_expires_at(0), "API key expires in 0 days"),
        (_supabase_expires_at(1), "API key expires in 1 day"),
        (_supabase_expires_at(7), "API key expires in 7 days"),
        (_supabase_expires_at(8), None),
        (_supabase_expires_at(-1), None),
        ("2026-08-11 17:23:14.401936+00", None),
        ("not-a-date", None),
    ],
)
def test_warn_if_api_key_expiring(expires_at, expected):
    with (
        patch(
            "lamindb_setup.core._hub_client.datetime", wraps=datetime
        ) as mock_datetime,
        patch("lamindb_setup.core._hub_client.logger.warning") as warning,
    ):
        mock_datetime.now.return_value = NOW
        _warn_if_api_key_expiring(expires_at)
    if expected is None:
        warning.assert_not_called()
    else:
        warning.assert_called_once_with(expected)
