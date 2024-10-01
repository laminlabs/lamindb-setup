import os

import httpx
import lamindb_setup as ln_setup


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
        assert instance["owner"] == "laminlabs"
        assert instance["name"] == "lamindata"
