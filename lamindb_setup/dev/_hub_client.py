# see the overlap with connector.py in laminhub-rest
import os
from typing import Optional
from urllib.request import urlretrieve

from lamin_utils import logger
from pydantic import BaseSettings
from supabase.lib.client_options import ClientOptions

from supabase import create_client


class Connector(BaseSettings):
    url: str
    key: str


def load_connector() -> Connector:
    if os.getenv("LAMIN_ENV") == "staging":
        url = "https://lamin-site-assets.s3.amazonaws.com/connector_staging.env"
    else:
        url = "https://lamin-site-assets.s3.amazonaws.com/connector.env"
    connector_file, _ = urlretrieve(url)
    connector = Connector(_env_file=connector_file)
    return connector


class Environment:
    def __init__(self):
        lamin_env = os.getenv("LAMIN_ENV")
        if lamin_env is None:
            lamin_env = "prod"
        if lamin_env in {"prod", "staging"}:
            connector = load_connector()
            supabase_api_url = connector.url
            supabase_anon_key = connector.key
        else:
            supabase_api_url = os.environ["SUPABASE_API_URL"]
            supabase_anon_key = os.environ["SUPABASE_ANON_KEY"]

        self.lamin_env: str = lamin_env
        self.supabase_api_url: str = supabase_api_url
        self.supabase_anon_key: str = supabase_anon_key


def connect_hub(client_options: ClientOptions = ClientOptions()):
    settings = Environment()
    return create_client(
        settings.supabase_api_url, settings.supabase_anon_key, client_options
    )


def connect_hub_with_auth(
    *,
    email: Optional[str] = None,
    password: Optional[str] = None,
    access_token: Optional[str] = None,
):
    hub = connect_hub()
    if access_token is None:
        if email is None or password is None:
            from lamindb_setup import settings

            email = settings.user.email
            password = settings.user.password
        access_token = get_access_token(email=email, password=password)
    hub.postgrest.auth(access_token)
    return hub


def get_access_token(email: Optional[str] = None, password: Optional[str] = None):
    hub = connect_hub()
    try:
        auth_response = hub.auth.sign_in_with_password(
            {
                "email": email,
                "password": password,
            }
        )
        return auth_response.session.access_token
    except Exception as e:
        logger.error(
            f"Could not authenticate with email {email} and password {password}"
        )
        raise e
    finally:
        hub.auth.sign_out()
