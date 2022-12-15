from lamin_logger import logger

# from ._hub import (
#     connect_hub_with_auth,
#     get_instance_info,
#     get_user_info_by_handle,
#     get_user_info_by_id,
# )
from ._settings import settings


def info():
    """Log information about current instance."""
    # Accessing cached settings is faster than accessing the hub
    logger.info(f"Instance: {settings.instance.owner}/{settings.instance.name}")
    # We'll need the code below for more comprehensive information
    # hub = connect_hub_with_auth()
    # owner_info = get_user_info_by_handle(hub, settings.instance.owner)
    # instance_info = get_instance_info(
    #     hub, settings.instance.name, owner_info.id.hex
    # )
    # hub.auth.sign_out()
