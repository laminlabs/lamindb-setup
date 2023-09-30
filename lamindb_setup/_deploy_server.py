import requests  # type: ignore

from ._settings import settings
from .dev._hub_client import Environment
from lamin_logger import logger


def deploy_server():
    """Deploy server for laminapp-rest."""
    _deploy_server(settings.instance.owner, settings.instance.name)
    return None


def _deploy_server(owner_handle: str, instance_name: str):
    response = requests.post(
        f"{Environment().hub_rest_server_url}/deploy/{owner_handle}/{instance_name}"
        "?cloud_run_instance_name_prefix=customer--",
        headers={"authentication": f"Bearer {settings.user.access_token}"},
    ).json()
    if response["status"] == "success":
        logger.info(response["message"])
    else:
        logger.error(response["message"])
