from typing import Optional
from pydantic import BaseModel, validator
from pydantic.networks import MultiHostDsn


def validate_schema_arg(schema: Optional[str] = None) -> str:
    if schema is None or schema == "":
        return ""
    # currently no actual validation, can add back if we see a need
    # the following just strips white spaces
    to_be_validated = [s.strip() for s in schema.split(",")]
    return ",".join(to_be_validated)


def validate_db_arg(db: Optional[str]) -> None:
    if db is not None:
        LaminDsnModel(db=db)


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
