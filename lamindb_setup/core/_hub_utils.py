from __future__ import annotations

from typing import Any, Optional

from pydantic import AnyUrl, BaseModel, ConfigDict, field_validator
from pydantic_core import CoreSchema, MultiHostUrl, core_schema


def validate_schema_arg(schema: str | None = None) -> str:
    if schema is None or schema == "":
        return ""
    # currently no actual validation, can add back if we see a need
    # the following just strips white spaces
    to_be_validated = [s.strip() for s in schema.split(",")]
    return ",".join(to_be_validated)


def validate_db_arg(db: str | None) -> None:
    if db is not None:
        LaminDsnModel(db=db)


class LaminDsn(AnyUrl):
    allowed_schemes = {
        "postgresql",
        # future enabled schemes
        # "snowflake",
        # "bigquery"
    }
    user_required = True

    @property
    def database(self):
        return self.path[1:]

    @classmethod
    def build(
        cls,
        *,
        scheme: str,
        user: str | None = None,
        password: str | None = None,
        host: str,
        port: str | None = None,
        database: str | None = None,
        query: str | None = None,
        fragment: str | None = None,
        **_kwargs: str,
    ) -> str:
        auth = f"{user}:{password}@" if user else ""
        port_str = f":{port}" if port else ""
        path = f"/{database}" if database else ""
        query_str = f"?{query}" if query else ""
        fragment_str = f"#{fragment}" if fragment else ""

        return f"{scheme}://{auth}{host}{port_str}{path}{query_str}{fragment_str}"

    @classmethod
    def validate(cls, value: Any) -> LaminDsn:
        if isinstance(value, str):
            return cls(value)
        elif isinstance(value, cls):
            return value
        else:
            raise ValueError(f"Invalid value for LaminDsn: {value}")

    @classmethod
    def __get_pydantic_core_schema__(cls, source_type: Any, handler: Any) -> CoreSchema:
        return core_schema.json_or_python_schema(
            json_schema=core_schema.str_schema(),
            python_schema=core_schema.union_schema(
                [
                    core_schema.is_instance_schema(cls),
                    core_schema.chain_schema(
                        [
                            core_schema.str_schema(),
                            core_schema.no_info_plain_validator_function(cls.validate),
                        ]
                    ),
                ]
            ),
            serialization=core_schema.plain_serializer_function_ser_schema(str),
        )


class LaminDsnModel(BaseModel):
    db: LaminDsn

    model_config = ConfigDict(arbitrary_types_allowed=True)

    @field_validator("db")
    @classmethod
    def check_db_name(cls, v):
        assert v.path and len(v.path) > 1, "database must be provided"
        return v
