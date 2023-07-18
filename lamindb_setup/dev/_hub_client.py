import os
from pathlib import Path
from typing import Any, Dict, Optional
from urllib.request import urlretrieve

from pydantic import BaseSettings, PostgresDsn
from supabase import create_client
from supabase.lib.client_options import ClientOptions


class Connector(BaseSettings):
    url: str
    key: str


class Settings(BaseSettings):
    lamin_env: str = "local"
    postgres_dsn: Optional[PostgresDsn]
    supabase_api_url: str
    supabase_anon_key: str
    supabase_service_role_key: Optional[str]
    ln_server_deploy: int = 0

    class Config:
        # This will look for an env file in the parent directory,
        # e.g. prod.env, staging.env, or local.env. Explicitly set
        # environment variables will take precendence over the ones
        # read in by file.

        env_file = (
            Path(__file__).parents[1] / f"lnhub-rest--{os.environ['LAMIN_ENV']}.env"
            if os.environ.get("LAMIN_ENV")
            else None
        )

        @classmethod
        def customise_sources(cls, init_settings, env_settings, file_secret_settings):
            # Following pattern here:
            # https://docs.pydantic.dev/usage/settings/#customise-settings-sources
            # in the absence of init and env settings, pull from S3
            return (init_settings, env_settings, lamindb_client_config_settings)


def connect_hub(client_options: ClientOptions = ClientOptions()):
    settings = Settings()

    return create_client(
        settings.supabase_api_url, settings.supabase_anon_key, client_options
    )
    # This is now handled in lamindb_client_config_settings
    # connector_file, _ = urlretrieve(
    #     "https://lamin-site-assets.s3.amazonaws.com/connector.env"
    # )
    # connector = Connector(_env_file=connector_file)
    # return create_client(connector.url, connector.key)


def connect_hub_with_auth(
    *,
    email: Optional[str] = None,
    password: Optional[str] = None,
    access_token: Optional[str] = None,
):
    hub = connect_hub()
    if access_token is None:
        if email is None or password is None:
            from .._settings_load import load_or_create_user_settings

            user_settings = load_or_create_user_settings()
            email = user_settings.email
            password = user_settings.password
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
    finally:
        hub.auth.sign_out()


def lamindb_client_config_settings(settings: BaseSettings) -> Dict[str, Any]:
    if os.getenv("LAMIN_ENV") == "staging":
        connector_path = (
            "https://lamin-site-assets.s3.amazonaws.com/connector_staging.env"
        )
    else:
        connector_path = "https://lamin-site-assets.s3.amazonaws.com/connector.env"
    connector_file, _ = urlretrieve(connector_path)
    connector = Connector(_env_file=connector_file)
    return dict(
        lamin_env="client",
        supabase_api_url=connector.url,
        supabase_anon_key=connector.key,
    )


def get_lamin_site_base_url():
    if "LAMIN_ENV" in os.environ:
        if os.environ["LAMIN_ENV"] == "local":
            return "http://localhost:3000"
        elif os.environ["LAMIN_ENV"] == "staging":
            return "https://staging.lamin.ai"
    return "https://lamin.ai"
