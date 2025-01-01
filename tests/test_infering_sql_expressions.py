from sqlalchemy import create_mock_engine, Column, Integer, Text
from sqlalchemy.orm import declarative_base

from atlas_provider_sqlalchemy_advanced import infer_sql_statement_from_object

engine = create_mock_engine("postgresql://", executor=lambda sql, *args, **kwargs: ...)


def test_infering_table_sql_expression():
    Base = declarative_base()

    class User(Base):
        __tablename__ = "users"
        __table_args__ = {"schema": "private"}

        id = Column(Integer, primary_key=True)
        username = Column(Text, unique=True)
        email = Column(Text)
        foo = Column(Text, nullable=False)

    test_entity = User.__table__

    sql_expression = infer_sql_statement_from_object(test_entity, engine)

    assert (
        sql_expression
        == """
CREATE TABLE private.users (
\tid SERIAL NOT NULL, 
\tusername TEXT, 
\temail TEXT, 
\tfoo TEXT NOT NULL, 
\tPRIMARY KEY (id), 
\tUNIQUE (username)
)

"""
    )


def test_infering_view_sql_expression():
    Base = declarative_base()

    class UserView(Base):
        __tablename__ = "user_view"
        __table_args__ = {
            "info": {
                "type": "view",
                "definition": "SELECT id, username, email FROM private.users",
            }
        }
        id = Column(Integer, primary_key=True)
        username = Column(Text, unique=True)
        email = Column(Text)

    test_entity = UserView.__table__

    sql_expression = infer_sql_statement_from_object(test_entity, engine)

    assert (
        sql_expression
        == "CREATE VIEW user_view AS SELECT id, username, email FROM private.users"
    )


def test_infering_materialized_view_sql_expression():
    Base = declarative_base()

    class UserMaterializedView(Base):
        __tablename__ = "user_view"
        __table_args__ = {
            "info": {
                "type": "materialized_view",
                "definition": "SELECT id, username, email FROM private.users",
            }
        }
        id = Column(Integer, primary_key=True)
        username = Column(Text, unique=True)
        email = Column(Text)

    test_entity = UserMaterializedView.__table__

    sql_expression = infer_sql_statement_from_object(test_entity, engine)

    assert (
        sql_expression
        == "CREATE MATERIALIZED VIEW user_view AS SELECT id, username, email FROM private.users"
    )
