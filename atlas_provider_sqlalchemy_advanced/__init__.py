import importlib
import inspect
from pathlib import Path

from sqlalchemy import create_mock_engine
from sqlalchemy.schema import CreateTable
from sqlalchemy.orm.decl_api import DeclarativeMeta


def infer_sql_statement_from_object(entity, engine) -> str:
    schema = entity.info.get("schema", "")
    name = entity.name
    definition = entity.info.get("definition")
    entity_type = entity.info.get("type")
    match entity_type:
        case "view":
            return f"CREATE VIEW {schema}{'.' if schema else ''}{name} AS {definition}"
        case "materialized_view":
            return f"CREATE MATERIALIZED VIEW {schema}{'.' if schema else ''}{name} AS {definition}"
        case _:  # Assume no type provided means table.
            return str(CreateTable(entity).compile(engine))


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

    entities = [e.__table__ for e in entities]

    unique_schemas = set()
    output_lines = []
    for current_entity in entities:
        # Add schema if it hasn't been added yet by another entity.
        current_schema = current_entity.schema
        if current_schema and current_schema not in unique_schemas:
            output_lines.append(f"CREATE SCHEMA IF NOT EXISTS {current_schema}")
            unique_schemas.add(current_schema)

        sql_statement = infer_sql_statement_from_object(current_entity, engine)
        output_lines.append(sql_statement.replace("\t", "").replace("\n", ""))

    return [f"{line};" for line in output_lines]
