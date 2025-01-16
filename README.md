# Atlas Provider: SqlAlchemy Advanced

This Atlas provider is based on the official [atlas-provider-sqlalchemy](https://github.com/ariga/atlas-provider-sqlalchemy/) but additionally allows defining database entities beyond tables directly in Python code without using workarounds like [Composite Schemas](https://atlasgo.io/atlas-schema/projects#data-source-composite_schema).

Instead, (materialized) views can be defined in an *orm.py* file like this:

```python
from atlas_provider_sqlalchemy_advanced.ddl import View

class User(Base):
    __tablename__ = "users"
    id = Column(Integer, primary_key=True)
    username = Column(Text, unique=True)
    email = Column(Text)
    foo = Column(Text, nullable=False)

class UserView(View, Base):
    __viewname__ = "user_view"
    __selectable__ = select(User.id, User.username, User.email)
```

Next, to create migrations in SQL format, configure Atlas like below:

```hcl
data "external_schema" "sqlalchemy" {
  program = ["python", "atlas_provider_sqlalchemy_advanced", "orm.py"]
}

env "sqlalchemy" {
  src = data.external_schema.sqlalchemy.url
  dev = "docker://postgres/16/dev"
  url = "postgres://postgres:password@localhost:5432/example?&sslmode=disable"
  migration {
    dir = "file://migrations"
  }
  format {
    migrate {
      diff = "{{ sql . \"  \" }}"
    }
  }
}
```

Then, run `atlas migrate diff --env sqlalchemy`.

## Development

Ideally, make sure that you have [uv](https://docs.astral.sh/uv/), [act](https://nektosact.com/), and [Task](https://taskfile.dev/) installed.
Then, you can use the following commands:
- `task tests` to run all unit tests.
- `task act` to run all GitHub Actions locally. (This requires you to first use `gh auth login` using the GitHub CLI.)
