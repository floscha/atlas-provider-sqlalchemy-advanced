import sys
from pathlib import Path

from atlas_provider_sqlalchemy_advanced import custom_create_all, get_entities_from_file

if __name__ == "__main__":
    orm_path = Path(sys.argv[1])
    entities = get_entities_from_file(orm_path)
    for line in custom_create_all(entities):
        print(line, end="\n\n")
