from __future__ import annotations

import hashlib
import importlib
import json
from typing import TYPE_CHECKING, Literal
from uuid import UUID

from django.db.models import (
    Field,
    ForeignKey,
    ForeignObjectRel,
    ManyToManyField,
    ManyToManyRel,
    ManyToOneRel,
    OneToOneField,
    OneToOneRel,
)
from pydantic import BaseModel

from lamindb_setup import settings
from lamindb_setup._init_instance import get_schema_module_name
from lamindb_setup.core._hub_client import call_with_fallback_auth

# surpress pydantic warning about `model_` namespace
try:
    BaseModel.model_config["protected_namespaces"] = ()
except Exception:
    pass


if TYPE_CHECKING:
    from supabase import Client


def update_schema_in_hub(access_token: str | None = None) -> tuple[bool, UUID, dict]:
    return call_with_fallback_auth(_synchronize_schema, access_token=access_token)


def _synchronize_schema(client: Client) -> tuple[bool, UUID, dict]:
    schema_metadata = _SchemaHandler()
    schema_metadata_dict = schema_metadata.to_json()
    schema_uuid = _dict_to_uuid(schema_metadata_dict)
    schema = _get_schema_by_id(schema_uuid, client)

    is_new = schema is None
    if is_new:
        module_set_info = schema_metadata._get_module_set_info()
        module_ids = "-".join(str(module_info["id"]) for module_info in module_set_info)
        schema = (
            client.table("schema")
            .insert(
                {
                    "id": schema_uuid.hex,
                    "module_ids": module_ids,
                    "module_set_info": module_set_info,
                    "schema_json": schema_metadata_dict,
                }
            )
            .execute()
            .data[0]
        )

    instance_response = (
        client.table("instance")
        .update({"schema_id": schema_uuid.hex})
        .eq("id", settings.instance._id.hex)
        .execute()
    )
    assert (
        len(instance_response.data) == 1
    ), f"schema of instance {settings.instance._id.hex} could not be updated with schema {schema_uuid.hex}"

    return is_new, schema_uuid, schema


def get_schema_by_id(id: UUID):
    return call_with_fallback_auth(_get_schema_by_id, id=id)


def _get_schema_by_id(id: UUID, client: Client):
    response = client.table("schema").select("*").eq("id", id.hex).execute()
    if len(response.data) == 0:
        return None
    return response.data[0]


def _dict_to_uuid(dict: dict):
    encoded = json.dumps(dict, sort_keys=True).encode("utf-8")
    hash = hashlib.md5(encoded).digest()
    uuid = UUID(bytes=hash[:16])
    return uuid


RelationType = Literal["many-to-one", "one-to-many", "many-to-many", "one-to-one"]
Type = Literal[
    "ForeignKey",
    # the following are generated with `from django.db import models; [attr for attr in dir(models) if attr.endswith('Field')]`
    "AutoField",
    "BigAutoField",
    "BigIntegerField",
    "BinaryField",
    "BooleanField",
    "CharField",
    "CommaSeparatedIntegerField",
    "DateField",
    "DateTimeField",
    "DecimalField",
    "DurationField",
    "EmailField",
    "Field",
    "FileField",
    "FilePathField",
    "FloatField",
    "GeneratedField",
    "GenericIPAddressField",
    "IPAddressField",
    "ImageField",
    "IntegerField",
    "JSONField",
    "ManyToManyField",
    "NullBooleanField",
    "OneToOneField",
    "PositiveBigIntegerField",
    "PositiveIntegerField",
    "PositiveSmallIntegerField",
    "SlugField",
    "SmallAutoField",
    "SmallIntegerField",
    "TextField",
    "TimeField",
    "URLField",
    "UUIDField",
]


class Through(BaseModel):
    left_key: str
    right_key: str
    link_table_name: str | None = None


class FieldMetadata(BaseModel):
    type: Type
    column_name: str | None = None
    through: Through | None = None
    field_name: str
    model_name: str
    schema_name: str
    is_link_table: bool
    is_primary_key: bool
    is_editable: bool
    relation_type: RelationType | None = None
    related_field_name: str | None = None
    related_model_name: str | None = None
    related_schema_name: str | None = None


class _ModelHandler:
    def __init__(self, model, module_name: str, included_modules: list[str]) -> None:
        from lamindb.models import LinkORM

        self.model = model
        self.class_name = model.__name__
        self.module_name = module_name
        self.model_name = model._meta.model_name
        self.table_name = model._meta.db_table
        self.included_modules = included_modules
        self.fields = self._get_fields_metadata(self.model)
        self.is_link_table = issubclass(model, LinkORM)

    def to_dict(self, include_django_objects: bool = True):
        _dict = {
            "fields": self.fields.copy(),
            "class_name": self.class_name,
            "table_name": self.table_name,
            "is_link_table": self.is_link_table,
        }

        for field_name in self.fields.keys():
            _dict["fields"][field_name] = _dict["fields"][field_name].__dict__
            through = _dict["fields"][field_name]["through"]
            if through is not None:
                _dict["fields"][field_name]["through"] = through.__dict__

        if include_django_objects:
            _dict.update({"model": self.model})

        return _dict

    def _get_fields_metadata(self, model):
        related_fields = []
        fields_metadata: dict[str, FieldMetadata] = {}

        for field in model._meta.get_fields():
            field_metadata = self._get_field_metadata(model, field)
            if field_metadata.related_schema_name is None:
                fields_metadata.update({field.name: field_metadata})

            if (
                field_metadata.related_schema_name in self.included_modules
                and field_metadata.schema_name in self.included_modules
            ):
                related_fields.append(field)

        related_fields_metadata = self._get_related_fields_metadata(
            model, related_fields
        )

        fields_metadata = {**fields_metadata, **related_fields_metadata}

        return fields_metadata

    def _get_related_fields_metadata(self, model, fields: list[ForeignObjectRel]):
        related_fields: dict[str, FieldMetadata] = {}

        for field in fields:
            if field.many_to_one:
                related_fields.update(
                    {f"{field.name}": self._get_field_metadata(model, field)}
                )
            elif field.one_to_many:
                # exclude self reference as it is already included in the many to one
                if field.related_model == model:
                    continue
                related_fields.update(
                    {f"{field.name}": self._get_field_metadata(model, field.field)}
                )
            elif field.many_to_many:
                related_fields.update(
                    {f"{field.name}": self._get_field_metadata(model, field)}
                )
            elif field.one_to_one:
                related_fields.update(
                    {f"{field.name}": self._get_field_metadata(model, field)}
                )

        return related_fields

    def _get_field_metadata(self, model, field: Field):
        from lamindb.models import LinkORM

        internal_type = field.get_internal_type()
        model_name = field.model._meta.model_name
        relation_type = self._get_relation_type(model, field)
        if field.related_model is None:
            schema_name = field.model.__get_module_name__()
            related_model_name = None
            related_schema_name = None
            related_field_name = None
            is_editable = field.editable
        else:
            related_model_name = field.related_model._meta.model_name
            related_schema_name = field.related_model.__get_module_name__()
            schema_name = field.model.__get_module_name__()
            related_field_name = field.remote_field.name
            is_editable = False

        field_name = field.name
        is_primary_key = getattr(field, "primary_key", False)

        if relation_type in ["one-to-many"]:
            # For a one-to-many relation, the field belong
            # to the other model as a foreign key.
            # To make usage similar to other relation types
            # we need to invert model and related model.
            schema_name, related_schema_name = related_schema_name, schema_name
            model_name, related_model_name = related_model_name, model_name
            field_name, related_field_name = related_field_name, field_name
            pass

        column = None
        if relation_type not in ["many-to-many", "one-to-many"]:
            if not isinstance(field, ForeignObjectRel):
                column = field.column

        if relation_type is None:
            through = None
        elif relation_type == "many-to-many":
            through = self._get_through_many_to_many(field)
        else:
            through = self._get_through(field)

        return FieldMetadata(
            schema_name=schema_name if schema_name != "lamindb" else "core",
            model_name=model_name,
            field_name=field_name,
            type=internal_type,
            is_link_table=issubclass(field.model, LinkORM),
            is_primary_key=is_primary_key,
            is_editable=is_editable,
            column_name=column,
            relation_type=relation_type,
            related_schema_name=related_schema_name
            if related_schema_name != "lamindb"
            else "core",
            related_model_name=related_model_name,
            related_field_name=related_field_name,
            through=through,
        )

    @staticmethod
    def _get_through_many_to_many(field_or_rel: ManyToManyField | ManyToManyRel):
        from lamindb.models import Registry

        if isinstance(field_or_rel, ManyToManyField):
            if field_or_rel.model != Registry:
                return Through(
                    left_key=field_or_rel.m2m_column_name(),
                    right_key=field_or_rel.m2m_reverse_name(),
                    link_table_name=field_or_rel.remote_field.through._meta.db_table,
                )
            else:
                return Through(
                    left_key=field_or_rel.m2m_reverse_name(),
                    right_key=field_or_rel.m2m_column_name(),
                    link_table_name=field_or_rel.remote_field.through._meta.db_table,
                )

        if isinstance(field_or_rel, ManyToManyRel):
            if field_or_rel.model != Registry:
                return Through(
                    left_key=field_or_rel.field.m2m_reverse_name(),
                    right_key=field_or_rel.field.m2m_column_name(),
                    link_table_name=field_or_rel.through._meta.db_table,
                )
            else:
                return Through(
                    left_key=field_or_rel.field.m2m_column_name(),
                    right_key=field_or_rel.field.m2m_reverse_name(),
                    link_table_name=field_or_rel.through._meta.db_table,
                )

    def _get_through(
        self, field_or_rel: ForeignKey | OneToOneField | ManyToOneRel | OneToOneRel
    ):
        if isinstance(field_or_rel, ForeignObjectRel):
            rel_1 = field_or_rel.field.related_fields[0][0]
            rel_2 = field_or_rel.field.related_fields[0][1]
        else:
            rel_1 = field_or_rel.related_fields[0][0]
            rel_2 = field_or_rel.related_fields[0][1]

        if rel_1.model._meta.model_name == self.model._meta.model_name:
            return Through(
                left_key=rel_1.column,
                right_key=rel_2.column,
            )
        else:
            return Through(
                left_key=rel_2.column,
                right_key=rel_1.column,
            )

    @staticmethod
    def _get_relation_type(model, field: Field):
        if field.many_to_one:
            # defined in the model
            if model == field.model:
                return "many-to-one"
            # defined in the related model
            else:
                return "one-to-many"
        elif field.one_to_many:
            return "one-to-many"
        elif field.many_to_many:
            return "many-to-many"
        elif field.one_to_one:
            return "one-to-one"
        else:
            return None


class _SchemaHandler:
    def __init__(self) -> None:
        self.included_modules = ["core"] + list(settings.instance.modules)
        self.modules = self._get_modules_metadata()

    def to_dict(self, include_django_objects: bool = True):
        return {
            module_name if module_name != "lamindb" else "core": {
                model_name: model.to_dict(include_django_objects)
                for model_name, model in module.items()
            }
            for module_name, module in self.modules.items()
        }

    def to_json(self):
        return self.to_dict(include_django_objects=False)

    def _get_modules_metadata(self):
        from lamindb.models import Record, Registry

        all_models = {
            module_name: {
                model._meta.model_name: _ModelHandler(
                    model, module_name, self.included_modules
                )
                for model in self._get_schema_module(
                    module_name
                ).models.__dict__.values()
                if model.__class__ is Registry
                and model is not Record
                and not model._meta.abstract
                and model.__get_module_name__() == module_name
            }
            for module_name in self.included_modules
        }
        assert all_models
        return all_models

    def _get_module_set_info(self):
        # TODO: rely on schemamodule table for this
        module_set_info = []
        for module_name in self.included_modules:
            module = self._get_schema_module(module_name)
            if module_name == "lamindb":
                module_name = "core"
            module_set_info.append(
                {"id": 0, "name": module_name, "version": module.__version__}
            )
        return module_set_info

    @staticmethod
    def _get_schema_module(module_name):
        return importlib.import_module(get_schema_module_name(module_name))
