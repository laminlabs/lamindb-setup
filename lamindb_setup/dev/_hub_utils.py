import secrets
import string
from pathlib import Path
from typing import Mapping, Optional, Union
from uuid import UUID

from pydantic import BaseModel, validator
from pydantic.networks import MultiHostDsn

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
    *, hub, db: Optional[str], storage_id: UUID, name: str, account: Mapping
) -> None:
    # if a remote sqlite instance, make sure there is no other instance
    # that has the same name and storage location
    if db is None:  # remote sqlite instance
        instances = (
            hub.table("instance")
            .select("*")
            .eq("storage_id", storage_id)
            .eq("name", name)
            .execute()
            .data
        )
        if len(instances) > 0:
            # retrieve account owning the first instance
            accounts = (
                hub.table("account")
                .select("*")
                .eq("id", instances[0]["account_id"])
                .execute()
                .data
            )
            raise RuntimeError(
                "\nThere is already an sqlite instance with the same name and storage"
                f" location from account {accounts[0]['handle']}\nTwo sqlite instances"
                " with the same name and the same storage cannot exist\nFix: "
                f"Choose another name or load instance {accounts[0]['handle']}/{name}\n"
            )


def get_storage_region(storage_root: Union[str, Path, UPath]) -> Optional[str]:
    storage_root_str = str(storage_root)
    storage_region = None

    if storage_root_str.startswith("s3://"):
        import botocore.session

        response = (
            botocore.session.get_session()
            .create_client("s3")
            .get_bucket_location(Bucket=storage_root_str.replace("s3://", ""))
        )
        # returns `None` for us-east-1
        # returns a string like "eu-central-1" etc. for all other regions
        storage_region = response["LocationConstraint"]
        if storage_region is None:
            storage_region = "us-east-1"

    return storage_region


def validate_storage_root_arg(storage_root: str) -> None:
    if storage_root.endswith("/"):
        raise ValueError("Pass settings.storage.root_as_str rather than path")
    if storage_root.startswith(("gs://", "s3://")):
        return None
    else:  # local path
        try:
            _ = Path(storage_root)
            return None
        except Exception:
            raise ValueError(
                "`storage` is neither a valid local, a Google Cloud nor an S3 path."
            )


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
            port=port,
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
