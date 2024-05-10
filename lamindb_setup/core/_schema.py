import os
from uuid import UUID

import requests  # type: ignore


def get_schema(instance_id: UUID) -> dict:
    from lamindb_setup import settings

    url = _get_rest_api_url()
    response = requests.get(
        f"{url}/schema/{str(instance_id)}",
        headers={"authentication": f"Bearer {settings.user.access_token}"},
    )
    assert response.status_code == 200
    return response.json()


def _get_rest_api_url():
    env = os.environ["LAMIN_ENV"]
    if env == "prod":
        return "https://w2pwdctwpt.us-east-1.awsapprunner.com"
    elif env == "prod-test":
        return "https://pfsxackcmb.us-east-1.awsapprunner.com"
    elif env == "staging":
        return "https://xkykpvjgnt.eu-central-1.awsapprunner.com"
    elif env == "staging-test":
        return "https://pyexev2ex3.eu-central-1.awsapprunner.com"
    return None
