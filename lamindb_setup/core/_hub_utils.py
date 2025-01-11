from __future__ import annotations

from typing import Any, ClassVar
from urllib.parse import urlparse, urlunparse

from pydantic import BaseModel, Field, GetCoremodulesHandler
from pydantic_core import Coremodules, core_modules


def validate_modules_arg(modules: str | None = None) -> str:
    if modules is None or modules == "":
        return ""
    # currently no actual validation, can add back if we see a need
    # the following just strips white spaces
    to_be_validated = [s.strip() for s in modules.split(",")]
    return ",".join(to_be_validated)


def validate_db_arg(db: str | None) -> None:
    if db is not None:
        LaminDsnModel(db=db)


class LaminDsn(str):
    allowed_schemes: ClassVar[set[str]] = {
        "postgresql",
        # future enabled schemes
        # "snowflake",
        # "bigquery"
    }

    @classmethod
    def __get_pydantic_core_modules__(
        cls, source_type: Any, handler: GetCoremodulesHandler
    ) -> Coremodules:
        return core_modules.no_info_after_validator_function(
            cls.validate,
            core_modules.str_modules(),
            serialization=core_modules.plain_serializer_function_ser_modules(str),
        )

    @classmethod
    def validate(cls, v: Any) -> LaminDsn:
        if isinstance(v, str):
            parsed = urlparse(v)
            if parsed.scheme not in cls.allowed_schemes:
                raise ValueError(f"Invalid scheme: {parsed.scheme}")
            return cls(v)
        elif isinstance(v, cls):
            return v
        else:
            raise ValueError(f"Invalid value for LaminDsn: {v}")

    @property
    def user(self) -> str | None:
        return urlparse(self).username

    @property
    def password(self) -> str | None:
        return urlparse(self).password

    @property
    def host(self) -> str | None:
        return urlparse(self).hostname

    @property
    def port(self) -> int | None:
        return urlparse(self).port

    @property
    def database(self) -> str:
        return urlparse(self).path.lstrip("/")

    @property
    def scheme(self) -> str:
        return urlparse(self).scheme

    @classmethod
    def build(
        cls,
        *,
        scheme: str,
        user: str | None = None,
        password: str | None = None,
        host: str,
        port: int | None = None,
        database: str | None = None,
        query: str | None = None,
        fragment: str | None = None,
    ) -> LaminDsn:
        netloc = host
        if port is not None:
            netloc = f"{netloc}:{port}"
        if user is not None:
            auth = user
            if password is not None:
                auth = f"{auth}:{password}"
            netloc = f"{auth}@{netloc}"

        path = f"/{database}" if database else ""

        url = urlunparse((scheme, netloc, path, "", query or "", fragment or ""))
        return cls(url)


class LaminDsnModel(BaseModel):
    db: LaminDsn = Field(..., description="The database DSN")

    model_config = {"arbitrary_types_allowed": True}
