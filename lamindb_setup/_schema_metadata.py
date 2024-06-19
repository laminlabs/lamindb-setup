from __future__ import annotations

import hashlib
import importlib
import json
from dataclasses import dataclass
from typing import TYPE_CHECKING, Dict
from uuid import UUID

import sqlparse
from django.contrib.postgres.expressions import ArraySubquery
from django.db.models import (
    Field,
    ForeignObjectRel,
    ManyToManyField,
    ManyToManyRel,
    OuterRef,
    QuerySet,
    Subquery,
)
from django.db.models.functions import JSONObject
from sqlparse.sql import Identifier, IdentifierList
from sqlparse.tokens import DML, Keyword

from lamindb_setup import settings
from lamindb_setup._init_instance import get_schema_module_name
from lamindb_setup.core._hub_client import call_with_fallback_auth

if TYPE_CHECKING:
    from lnschema_core.models import Registry
    from supabase import Client


def update_schema_in_hub() -> tuple[bool, UUID, dict]:
    return call_with_fallback_auth(_synchronize_schema)


def _synchronize_schema(client: Client) -> tuple[bool, UUID, dict]:
    schema_metadata = SchemaMetadata()
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
                    "json": schema_metadata_dict,
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


class SchemaMetadata:
    def __init__(self) -> None:
        self.included_modules = ["core"] + list(settings.instance.schema)
        self.modules = self._get_modules_metadata()

    def to_dict(
        self, include_django_objects: bool = True, include_select_terms: bool = True
    ):
        return {
            module_name: {
                model_name: model.to_dict(include_django_objects, include_select_terms)
                for model_name, model in module.items()
            }
            for module_name, module in self.modules.items()
        }

    def to_json(self, include_select_terms: bool = True):
        return self.to_dict(
            include_django_objects=False, include_select_terms=include_select_terms
        )

    def _get_modules_metadata(self):
        return {
            module_name: {
                model._meta.model_name: ModelMetadata(
                    model, module_name, self.included_modules
                )
                for model in self._get_schema_module(
                    module_name
                ).models.__dict__.values()
                if model.__class__.__name__ == "ModelBase"
                and model.__name__ not in ["Registry", "ORM"]
                and not model._meta.abstract
                and model.__get_schema_name__() == module_name
            }
            for module_name in self.included_modules
        }

    def _get_module_set_info(self):
        # TODO: rely on schemamodule table for this
        module_set_info = []
        for module_name in self.included_modules:
            module = self._get_schema_module(module_name)
            module_set_info.append(
                {"id": 0, "name": module_name, "version": module.__version__}
            )
        return module_set_info

    @staticmethod
    def _get_schema_module(module_name):
        return importlib.import_module(get_schema_module_name(module_name))


@dataclass
class FieldMetadata:
    schema_name: str
    model_name: str
    field_name: str
    type: str
    is_link_table: bool
    column: str | None = None
    relation_type: str | None = None
    related_schema_name: str | None = None
    related_model_name: str | None = None
    related_field_name: str | None = None
    through: dict | None = None


class ModelRelations:
    def __init__(self, fields: list[ForeignObjectRel]) -> None:
        self.many_to_one = {}
        self.one_to_many = {}
        self.many_to_many = {}
        self.one_to_one = {}

        for field in fields:
            if field.many_to_one:
                self.many_to_one.update({field.name: field})
            elif field.one_to_many:
                self.one_to_many.update({field.name: field})
            elif field.many_to_many:
                self.many_to_many.update({field.name: field})
            elif field.one_to_one:
                self.one_to_one.update({field.name: field})

        self.all = {
            **self.many_to_one,
            **self.one_to_many,
            **self.many_to_many,
            **self.one_to_one,
        }


class ModelMetadata:
    def __init__(self, model, module_name: str, included_modules: list[str]) -> None:
        self.model = model
        self.class_name = model.__name__
        self.module_name = module_name
        self.model_name = model._meta.model_name
        self.table_name = model._meta.db_table
        self.included_modules = included_modules
        self.fields, self.relations = self._get_fields_metadata(self.model)

    def to_dict(
        self, include_django_objects: bool = True, include_select_terms: bool = True
    ):
        _dict = {
            "fields": self.fields.copy(),
            "class_name": self.class_name,
            "table_name": self.table_name,
        }

        select_terms = self.select_terms if include_select_terms else []

        for field_name in self.fields.keys():
            _dict["fields"][field_name] = _dict["fields"][field_name].__dict__
            if field_name in select_terms:
                _dict["fields"][field_name].update(
                    {"select_term": select_terms[field_name]}
                )

        if include_django_objects:
            _dict.update({"model": self.model})

        return _dict

    @property
    def select_terms(self):
        return (
            DjangoQueryBuilder(self.module_name, self.model_name)
            .add_all_sub_queries()
            .extract_select_terms()
        )

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

        model_relations_metadata = ModelRelations(related_fields)

        related_fields_metadata = self._get_related_fields_metadata(
            model, model_relations_metadata
        )

        fields_metadata = {**fields_metadata, **related_fields_metadata}

        return fields_metadata, model_relations_metadata

    def _get_related_fields_metadata(
        self, model, model_relations_metadata: ModelRelations
    ):
        related_fields: dict[str, FieldMetadata] = {}

        # Many to one (foreign key defined in the model)
        for link_field_name, link_field in model_relations_metadata.many_to_one.items():
            related_fields.update(
                {f"{link_field_name}": self._get_field_metadata(model, link_field)}
            )
            for field in link_field.related_model._meta.fields:
                related_fields.update(
                    {
                        f"{link_field_name}__{field.name}": self._get_field_metadata(
                            model, field
                        )
                    }
                )

        # One to many (foreign key defined in the related model)
        for relation_name, relation in model_relations_metadata.one_to_many.items():
            # exclude self reference as it is already included in the many to one
            if relation.related_model == model:
                continue
            related_fields.update(
                {f"{relation_name}": self._get_field_metadata(model, relation.field)}
            )

        # One to one
        for link_field_name, link_field in model_relations_metadata.one_to_one.items():
            related_fields.update(
                {f"{link_field_name}": self._get_field_metadata(model, link_field)}
            )
            for field in link_field.related_model._meta.fields:
                related_fields.update(
                    {
                        f"{link_field_name}__{field.name}": self._get_field_metadata(
                            model, field
                        )
                    }
                )

        # Many to many
        for (
            link_field_name,
            link_field,
        ) in model_relations_metadata.many_to_many.items():
            related_fields.update(
                {f"{link_field_name}": self._get_field_metadata(model, link_field)}
            )

        return related_fields

    def _get_field_metadata(self, model, field: Field):
        from lnschema_core.models import LinkORM

        internal_type = field.get_internal_type()
        model_name = field.model._meta.model_name
        relation_type = self._get_relation_type(model, field)
        if field.related_model is None:
            schema_name = field.model.__get_schema_name__()
            related_model_name = None
            related_schema_name = None
            related_field_name = None
            field_name = field.name
        else:
            related_model_name = field.related_model._meta.model_name
            related_schema_name = field.related_model.__get_schema_name__()
            schema_name = field.model.__get_schema_name__()
            related_field_name = field.remote_field.name
            field_name = field.name

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
        if relation_type not in ["many-to-many", "one-to-one", "one-to-many"]:
            column = field.column

        through = None
        if relation_type == "many-to-many":
            through = self._get_through(model, field)

        return FieldMetadata(
            schema_name,
            model_name,
            field_name,
            internal_type,
            issubclass(field.model, LinkORM),
            column,
            relation_type,
            related_schema_name,
            related_model_name,
            related_field_name,
            through,
        )

    @staticmethod
    def _get_through(model, field_or_rel: ManyToManyField | ManyToManyRel):
        table_name = model._meta.db_table
        related_table_name = field_or_rel.related_model._meta.db_table

        if isinstance(field_or_rel, ManyToManyField):
            return {
                "link_table_name": field_or_rel.remote_field.through._meta.db_table,
                table_name: field_or_rel.m2m_column_name(),
                related_table_name: field_or_rel.m2m_reverse_name(),
            }

        if isinstance(field_or_rel, ManyToManyRel):
            return {
                "link_table_name": field_or_rel.through._meta.db_table,
                table_name: field_or_rel.field.m2m_column_name(),
                related_table_name: field_or_rel.field.m2m_reverse_name(),
            }

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


class DjangoQueryBuilder:
    def __init__(
        self, module_name: str, model_name: str, query_set: QuerySet | None = None
    ) -> None:
        self.schema_metadata = SchemaMetadata()
        self.module_name = module_name
        self.model_name = model_name
        self.model_metadata = self.schema_metadata.modules[module_name][model_name]
        self.query_set = query_set if query_set else self.model_metadata.model.objects

    def add_all_sub_queries(self):
        all_fields = self.model_metadata.fields
        included_relations = [
            field_name
            for field_name, field in all_fields.items()
            if field.relation_type is not None
        ]
        self.add_sub_queries(included_relations)
        return self

    def add_sub_queries(self, included_relations: list[str]):
        sub_queries = {
            f"annotated_{relation_name}": self._get_sub_query(
                self.model_metadata.fields[relation_name]
            )
            for relation_name in included_relations
        }
        self.query_set = self.query_set.annotate(**sub_queries)
        return self

    def extract_select_terms(self):
        parsed = sqlparse.parse(self.sql_query)
        select_found = False
        select_terms = {}

        def get_name(identifier):
            name = identifier.get_name()
            return name if name is not None else str(identifier).split(".")

        for token in parsed[0].tokens:
            if token.ttype is DML and token.value.upper() == "SELECT":
                select_found = True
            elif select_found and isinstance(token, IdentifierList):
                for identifier in token.get_identifiers():
                    select_terms[get_name(identifier)] = str(identifier)
            elif select_found and isinstance(token, Identifier):
                select_terms[get_name(token)] = str(token)
            elif token.ttype is Keyword:
                if token.value.upper() in ["FROM", "WHERE", "GROUP BY", "ORDER BY"]:
                    break

        return select_terms

    def _get_sub_query(self, field_metadata: FieldMetadata):
        module_name = field_metadata.related_schema_name
        model_name = field_metadata.related_model_name
        field_name = field_metadata.related_field_name
        model_metadata = self.schema_metadata.modules[module_name][model_name]
        query_set = model_metadata.model.objects.get_queryset()
        select = {
            field_name: field_name
            for field_name in model_metadata.fields.keys()
            if model_metadata.fields[field_name].relation_type is None
            and "__" not in field_name
        }

        if field_metadata.relation_type in ["many-to-many", "one-to-many"]:
            return ArraySubquery(
                Subquery(
                    query_set.filter(**{field_name: OuterRef("pk")}).values(
                        data=JSONObject(**select)
                    )[:5]
                )
            )
        if field_metadata.relation_type in ["many-to-one", "one-to-one"]:
            return Subquery(
                query_set.filter(**{field_name: OuterRef("pk")}).values(
                    data=JSONObject(**select)
                )[:5]
            )

    @property
    def sql_query(self):
        sql_template, params = self.query_set.query.sql_with_params()
        sql_query = sql_template % tuple(f"'{p}'" for p in params)
        return sql_query.replace("annotated_", "")
