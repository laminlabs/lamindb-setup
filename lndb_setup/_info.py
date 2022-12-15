from lamin_logger import logger

from ._hub import connect_hub_with_auth, get_instance_info, get_user_info_by_id
from ._settings import settings


def info():
    """Log information about current instance."""
    hub = connect_hub_with_auth()
    instance_info = get_instance_info(
        hub, settings.instance.name, hub.auth.session().user.id.hex
    )
    user_info = get_user_info_by_id(hub, instance_info["owner_id"])
    handle = user_info["handle"]
    logger.info(f"Instance: {handle}/{settings.instance.name}")
    hub.auth.sign_out()
