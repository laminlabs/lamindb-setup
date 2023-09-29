import requests  # type: ignore

from ._settings import settings
from .dev._hub_client import Environment


def deploy_server():
    """Deploy server for laminapp-rest."""
    _deploy_server(settings.user.handle, settings.instance.name)
    return None


def _deploy_server(owner_handle: str, instance_name: str):
    requests.post(
        f"{Environment().hub_rest_server_url}/deploy/{owner_handle}/{instance_name}",
        headers={"authentication": f"Bearer {settings.user.access_token}"},
    )
