import importlib
import inspect
from pathlib import Path

from sqlalchemy import create_mock_engine
from sqlalchemy.schema import CreateTable
from sqlalchemy.orm.decl_api import DeclarativeMeta

from atlas_provider_sqlalchemy_advanced.ddl.view import CreateView, View
from atlas_provider_sqlalchemy_advanced.ddl.materialized_view import CreateMaterializedView, MaterializedView


def infer_sql_statement_from_object(entity, engine) -> str:
    try:
        bases = (entity.__bases__)
    except AttributeError:
        bases = ()

    if View in bases:
        args = entity.__view_args__
        return str(CreateView(args.get("schema"), entity.__viewname__, entity.__selectable__))
    elif MaterializedView in bases:
        args = entity.__view_args__
        return str(CreateMaterializedView(args.get("schema"), entity.__viewname__, entity.__selectable__))
    else:
        return str(CreateTable(entity.__table__).compile(engine))


def get_entities_from_file(file_path: Path):
    module_spec = importlib.util.spec_from_file_location(file_path.stem, file_path)
    module = importlib.util.module_from_spec(module_spec)
    module_spec.loader.exec_module(module)
    return [
        v
        for (name, v) in inspect.getmembers(module)
        if isinstance(v, DeclarativeMeta) and name != "Base"
    ]


def custom_create_all(entities: list) -> list[str]:
    """Custom function to create SQL CREATE statements for tables and views."""
    engine = create_mock_engine(
        "postgresql://", executor=lambda sql, *args, **kwargs: ...
    )

    unique_schemas = set()
    output_lines = []
    for current_entity in entities:
        # Add schema if it hasn't been added yet by another entity.
        current_schema = current_entity.__table__.schema
        if current_schema and current_schema not in unique_schemas:
            output_lines.append(f"CREATE SCHEMA IF NOT EXISTS {current_schema}")
            unique_schemas.add(current_schema)

        sql_statement = infer_sql_statement_from_object(current_entity, engine)
        output_lines.append(sql_statement.replace("\t", "").replace("\n", ""))

    return [f"{line};" for line in output_lines]
