import sqlalchemy as sa
from sqlalchemy.ext import compiler
from sqlalchemy.schema import DDLElement
from sqlalchemy.sql import table
from sqlalchemy.orm import declared_attr


class CreateMaterializedView(DDLElement):
    def __init__(self, schema, name, selectable):
        self.schema = schema
        self.name = name
        self.selectable = selectable


class DropMaterializedView(DDLElement):
    def __init__(self, schema, name):
        self.schema = schema
        self.name = name


@compiler.compiles(CreateMaterializedView)
def _create_view(element, compiler, **kw):
    return "CREATE MATERIALIZED VIEW %s%s AS %s" % (
        element.schema + "." if element.schema else "",
        element.name,
        compiler.sql_compiler.process(element.selectable, literal_binds=True),
    )


@compiler.compiles(DropMaterializedView)
def _drop_view(element, compiler, **kw):
    return "DROP VIEW %s.%s" % (element.schema, element.name)


def create_view_table(schema: str, name: str, metadata: sa.MetaData, selectable: sa.Selectable) -> sa.Table:
    def view_exists(ddl, target, connection, **kw):
        return ddl.name in sa.inspect(connection).get_view_names()
    def view_doesnt_exist(ddl, target, connection, **kw):
        return not view_exists(ddl, target, connection, **kw)
    sa.event.listen(metadata, "after_create", CreateMaterializedView(schema, name, selectable).execute_if(callable_=view_doesnt_exist))
    sa.event.listen(metadata, "before_drop", DropMaterializedView(schema, name).execute_if(callable_=view_exists))

    t = table(name, schema=schema)
    t._columns._populate_separate_keys(col._make_proxy(t) for col in selectable.selected_columns)
    return t


class MaterializedView:
    __view_args__ = {}

    @declared_attr.directive
    def __table__(cls) -> sa.Table:
        return create_view_table(cls.__view_args__.get("schema"), cls.__viewname__, cls.metadata, cls.__selectable__)
