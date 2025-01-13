import sqlalchemy as sa
from sqlalchemy.orm import declarative_base

from atlas_provider_sqlalchemy_advanced.ddl import View


def test_view_ddl():
    engine = sa.create_engine("postgresql+psycopg2://postgres:password@localhost:5432/example?&sslmode=disable")

    Base = declarative_base()

    class Stuff(Base):
        __tablename__ = "stuff"
        __table_args__ = {"schema": "demo"}

        id = sa.Column(sa.Integer, primary_key=True)
        data = sa.Column(sa.Text)

        def __repr__(self):
            return f"Stuff(id={self.id}, data={self.data})"
    
    class MoreStuff(Base):
        __tablename__ = "more_stuff"
        __table_args__ = {"schema": "demo"}

        id = sa.Column(sa.Integer, primary_key=True)
        stuff_id = sa.Column(sa.Integer, sa.ForeignKey("demo.stuff.id"))
        data = sa.Column(sa.Text)

        def __repr__(self):
            return f"MoreStuff(id={self.id}, stuff_id={self.stuff_id}, data={self.data})"
    
    class StuffView(View, Base):
        __viewname__ = "stuff_view"
        __view_args__ = {"schema": "demo"}
        __selectable__ = (
            sa.select(
                Stuff.id.label("id"),
                Stuff.data.label("data"),
                MoreStuff.data.label("moredata"),
            )
            .join_from(Stuff, MoreStuff)
            .where(Stuff.data.like(("%orange%")))
        )

        def __repr__(self):
            return f"{self.__table__.name}({self.id!r}, {self.data!r}, {self.moredata!r})"

    assert StuffView.__table__.primary_key == [StuffView.id]

    with engine.begin() as conn:
        conn.execute(sa.text("DROP SCHEMA IF EXISTS demo CASCADE"))
        conn.execute(sa.text("CREATE SCHEMA demo"))
        Base.metadata.create_all(conn)

        conn.execute(sa.insert(Stuff), [
            {"data": "apples"},
            {"data": "pears"},
            {"data": "oranges"},
            {"data": "orange julius"},
            {"data": "apple jacks"},
        ])
        conn.execute(sa.insert(MoreStuff), [
            {"stuff_id": 3, "data": "foobar"},
            {"stuff_id": 4, "data": "foobar"},
        ])

    with engine.connect() as conn:
        view_result = conn.execute(sa.select(StuffView.data, StuffView.moredata)).all()
    assert view_result == [("oranges", "foobar"), ("orange julius", "foobar")]
