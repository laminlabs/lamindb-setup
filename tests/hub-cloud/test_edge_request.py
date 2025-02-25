import os
from uuid import UUID

import httpx
import lamindb_setup as ln_setup
from lamindb_setup.core.hashing import hash_and_encode_as_b62


# This shows how to make a request to execute get-instance-settings-v1 edge function
# This edge function is used in lamindb_setup.core._hub_core.connect_instance_hub
# connect_instance_hub is tested in hub-local tests, in test_all.py test_connect_instance_hub
def test_edge_request():
    if os.environ["LAMIN_ENV"] == "prod":
        # login to have access_token in ln_setup.settings.user
        ln_setup.login("testuser1@lamin.ai")
        # this is hub prod url
        # normally it is configured through
        # lamindb_setup.core._hub_client.Environment
        supabase_url = "https://hub.lamin.ai"
        # edge function to call
        function_name = "get-instance-settings-v1"
        url = f"{supabase_url}/functions/v1/{function_name}"
        # Data to send in the request body
        # arguments of the edge function
        body = {
            "owner": "laminlabs",  # owner of the instance
            "name": "lamindata",  # name of the instance
        }
        # Headers for authorization
        # we use access_token of the current user
        headers = {
            "Authorization": f"Bearer {ln_setup.settings.user.access_token}",
            "Content-Type": "application/json",
        }
        # Make the POST request
        response = httpx.post(url, json=body, headers=headers)
        instance = response.json()
        # instance id
        assert instance["id"] == "037ba1e0-8d80-4f91-a902-75a47735076a"
        assert "lnid" in instance
        # check correctness of lnid
        assert instance["lnid"] == hash_and_encode_as_b62(UUID(instance["id"]).hex)[:12]
        assert instance["owner"] == "laminlabs"
        assert instance["name"] == "lamindata"
        assert instance["api_url"] == "https://aws.us-east-1.lamin.ai/api"
        assert instance["schema_str"].startswith("bionty,wetlab")
        assert "schema_id" in instance
        # check that schema_id is well-formed UUID
        # if not, throws ValueError
        UUID(instance["schema_id"])
        assert "git_repo" in instance
        assert "keep_artifacts_local" in instance
        assert "lamindb_version" in instance
        # this is a dict with default storage info
        assert "storage" in instance
        # db related info
        assert "db_scheme" in instance
        assert "db_host" in instance
        assert "db_port" in instance
        assert "db_database" in instance
        assert "db_permissions" in instance
        assert "db_user_name" in instance
        assert "db_user_password" in instance
