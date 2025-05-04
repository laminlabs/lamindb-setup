from __future__ import annotations

import os

from laminhub_rest.dev import (
    SupabaseResources,
    remove_lamin_local_settings,
    seed_local_test,
)

supabase_resources = SupabaseResources()


pytest_plugins = [
    "laminhub_rest.test.account.fixtures",
    "laminhub_rest.test.common_fixtures",
]


def pytest_configure():
    os.environ["LAMIN_ENV"] = "local"
    os.environ["LAMIN_CLOUD_VERSION"] = "0.1"
    remove_lamin_local_settings()
    supabase_resources.start_local()
    supabase_resources.reset_local()
    supabase_resources.migrate()
    seed_local_test()


def pytest_unconfigure():
    if supabase_resources.edge_function_process:
        supabase_resources.stop_local_edge_functions()
