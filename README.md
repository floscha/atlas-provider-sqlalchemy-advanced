# Atlas Provider: SqlAlchemy Advanced

This Atlas provider is based on the official [atlas-provider-sqlalchemy](https://github.com/ariga/atlas-provider-sqlalchemy/) but additionally allows defining database entities beyond tables directly in Python code without using workarounds like [Composite Schemas](https://atlasgo.io/atlas-schema/projects#data-source-composite_schema).
Instead, the information is provided through SqlAlchemy's built-in [\_\_table_args\_\_](https://docs.sqlalchemy.org/en/20/orm/mapping_api.html#sqlalchemy.orm.DeclarativeBase.__table_args__):

```python
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
```