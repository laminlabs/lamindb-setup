import secrets
import string
from pathlib import Path
from typing import Optional, Union
from uuid import UUID
from lamin_utils import logger
from pydantic import BaseModel, validator
from pydantic.networks import MultiHostDsn

from supabase import Client

from .upath import UPath


def base62(n_char: int) -> str:
    """Like nanoid without hyphen and underscore."""
    alphabet = string.digits + string.ascii_letters.swapcase()
    id = "".join(secrets.choice(alphabet) for i in range(n_char))
    return id


def secret() -> str:
    """Password or secret: 40 base62."""
    return base62(n_char=40)


def validate_schema_arg(schema: Optional[str] = None) -> str:
    if schema is None or schema == "":
        return ""
    # currently no actual validation, can add back if we see a need
    # the following just strips white spaces
    to_be_validated = [s.strip() for s in schema.split(",")]
    return ",".join(to_be_validated)


def validate_db_arg(db: Optional[str]) -> None:
    if db:
        LaminDsnModel(db=db)


def validate_unique_sqlite(
    *,
    storage_id: UUID,
    name: str,
    client: Client,
) -> None:
    # if a remote sqlite instance, make sure there is no other instance
    # that has the same name and storage location
    instances = (
        client.table("instance")
        .select("*")
        .eq("storage_id", storage_id.hex)
        .eq("name", name)
        .execute()
        .data
    )
    if len(instances) > 0:
        # retrieve account owning the first instance
        accounts = (
            client.table("account")
            .select("*")
            .eq("id", instances[0]["account_id"])
            .execute()
            .data
        )
        raise RuntimeError(
            "\nThere is already an sqlite instance with the same name and storage"
            f" location from account {accounts[0]['handle']}\nTwo sqlite instances with"
            " the same name and the same storage cannot exist\nFix: Choose another"
            f" name or load instance {accounts[0]['handle']}/{name}\nFor reference: the"
            f" instances with the same storage and name are {instances}"
        )


def get_storage_region(storage_root: Union[str, Path, UPath]) -> Optional[str]:
    storage_root_str = str(storage_root)
    if storage_root_str.startswith("s3://"):
        import botocore.session as session
        from botocore.config import Config
        from botocore.exceptions import NoCredentialsError

        bucket = storage_root_str.replace("s3://", "")
        s3_session = session.get_session()
        s3_client = s3_session.create_client("s3")
        try:
            response = s3_client.head_bucket(Bucket=bucket)
        except NoCredentialsError:  # deal with anonymous access
            s3_client = s3_session.create_client(
                "s3", config=Config(signature_version=session.UNSIGNED)
            )
            response = s3_client.head_bucket(Bucket=bucket)
        storage_region = response["ResponseMetadata"].get("HTTPHeaders", {})[
            "x-amz-bucket-region"
        ]
        # if we want to except botcore.exceptions.ClientError to reformat an
        # error message, this is how to do test for the "NoSuchBucket" error:
        #     exc.response["Error"]["Code"] == "NoSuchBucket"
    else:
        storage_region = None
    return storage_region


def validate_storage_root_arg(storage_root: str) -> None:
    if storage_root.startswith(("gs://", "s3://")):
        # check for existence happens in get_storage_region
        return None
    else:  # local path
        try:
            _ = Path(storage_root)
            return None
        except Exception as e:
            logger.error(
                "`storage` is neither a valid local, a Google Cloud nor an S3 path."
            )
            raise e


def get_storage_type(storage_root: str):
    if str(storage_root).startswith("s3://"):
        return "s3"
    elif str(storage_root).startswith("gs://"):
        return "gs"
    else:
        return "local"


class LaminDsn(MultiHostDsn):
    """Custom DSN Type for Lamin.

    This class allows us to customize the allowed schemes for databases
    and also handles the parsing and building of DSN strings with the
    database name instead of URL path.
    """

    allowed_schemes = {
        "postgresql",
        # future enabled schemes
        # "snowflake",
        # "bigquery"
    }
    user_required = True
    __slots__ = ()

    @property
    def database(self):
        return self.path[1:]

    @classmethod
    def build(
        cls,
        *,
        scheme: str,
        user: Optional[str] = None,
        password: Optional[str] = None,
        host: str,
        port: Optional[str] = None,
        database: Optional[str] = None,
        query: Optional[str] = None,
        fragment: Optional[str] = None,
        **_kwargs: str,
    ) -> str:
        return super().build(
            scheme=scheme,
            user=user,
            password=password,
            host=host,
            port=str(port),
            path=f"/{database}",
            query=query,
            fragment=fragment,
        )


class LaminDsnModel(BaseModel):
    db: LaminDsn

    @validator("db")
    def check_db_name(cls, v):
        assert v.path and len(v.path) > 1, "database must be provided"
        return v
