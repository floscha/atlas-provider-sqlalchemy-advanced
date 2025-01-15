from sqlalchemy import create_mock_engine, Column, Integer, Text, ForeignKey, select
from sqlalchemy.orm import declarative_base

from atlas_provider_sqlalchemy_advanced import infer_sql_statement_from_object
from atlas_provider_sqlalchemy_advanced.ddl import MaterializedView, View

engine = create_mock_engine("postgresql://", executor=lambda sql, *args, **kwargs: ...)


def test_infering_table_sql_expression_without_schema():
    Base = declarative_base()

    class User(Base):
        __tablename__ = "users"

        id = Column(Integer, primary_key=True)
        username = Column(Text, unique=True)
        email = Column(Text)
        foo = Column(Text, nullable=False)

    sql_expression = infer_sql_statement_from_object(User, engine)

    assert (
        sql_expression
        == """
CREATE TABLE users (
    id SERIAL NOT NULL, 
    username TEXT, 
    email TEXT, 
    foo TEXT NOT NULL, 
    PRIMARY KEY (id), 
    UNIQUE (username)
)\n\n""".replace("    ", "\t"))

def test_infering_table_sql_expression_with_schema():
    Base = declarative_base()

    class User(Base):
        __tablename__ = "users"
        __table_args__ = {"schema": "private"}

        id = Column(Integer, primary_key=True)
        username = Column(Text, unique=True)
        email = Column(Text)
        foo = Column(Text, nullable=False)

    sql_expression = infer_sql_statement_from_object(User, engine)

    assert (
        sql_expression
        == """
CREATE TABLE private.users (
    id SERIAL NOT NULL, 
    username TEXT, 
    email TEXT, 
    foo TEXT NOT NULL, 
    PRIMARY KEY (id), 
    UNIQUE (username)
)\n\n""".replace("    ", "\t"))


def test_foreign_key_with_on_delete_cascade():
    Base = declarative_base()

    class Table1(Base):
        __tablename__ = "table1"
        id = Column(Integer, primary_key=True)

    class Table2(Base):
        __tablename__ = "table2"
        id = Column(Integer, primary_key=True)
        fk_id = Column(Integer, ForeignKey("table1.id", ondelete="CASCADE"))

    sql_expression_table_2 = infer_sql_statement_from_object(Table2, engine)
    assert(sql_expression_table_2 == """
CREATE TABLE table2 (
    id SERIAL NOT NULL, 
    fk_id INTEGER, 
    PRIMARY KEY (id), 
    FOREIGN KEY(fk_id) REFERENCES table1 (id) ON DELETE CASCADE
)\n\n""".replace("    ", "\t"))


def test_infering_view_sql_expression_without_schema():
    Base = declarative_base()

    class User(Base):
        __tablename__ = "users"

        id = Column(Integer, primary_key=True)
        username = Column(Text, unique=True)
        email = Column(Text)
        foo = Column(Text, nullable=False)

    class UserView(View, Base):
        __viewname__ = "user_view"
        __selectable__ = select(User.id, User.username, User.email)

    sql_expression = infer_sql_statement_from_object(UserView, engine)

    assert (
        sql_expression
        == "CREATE VIEW user_view AS SELECT users.id, users.username, users.email \nFROM users"
    )

def test_infering_view_sql_expression_with_schema():
    Base = declarative_base()

    class User(Base):
        __tablename__ = "users"

        id = Column(Integer, primary_key=True)
        username = Column(Text, unique=True)
        email = Column(Text)
        foo = Column(Text, nullable=False)

    class UserView(View, Base):
        __viewname__ = "user_view"
        __view_args__ = {"schema": "private"}
        __selectable__ = select(User.id, User.username, User.email)

    sql_expression = infer_sql_statement_from_object(UserView, engine)

    assert (
        sql_expression
        == "CREATE VIEW private.user_view AS SELECT users.id, users.username, users.email \nFROM users"
    )

def test_infering_materialized_view_sql_expression():
    Base = declarative_base()

    class User(Base):
        __tablename__ = "users"

        id = Column(Integer, primary_key=True)
        username = Column(Text, unique=True)
        email = Column(Text)
        foo = Column(Text, nullable=False)

    class UserView(MaterializedView, Base):
        __viewname__ = "user_view"
        __selectable__ = select(User.id, User.username, User.email)

    sql_expression = infer_sql_statement_from_object(UserView, engine)

    assert (
        sql_expression
        == "CREATE MATERIALIZED VIEW user_view AS SELECT users.id, users.username, users.email \nFROM users"
    )
