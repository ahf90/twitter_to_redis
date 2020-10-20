import os
from sqlalchemy.ext.automap import automap_base
from sqlalchemy import create_engine


def connect_to_db():
    db_username = os.getenv('DB_USER')
    db_password = os.getenv('DB_PASS')
    db_url = os.getenv('DB_URL')
    db_port = os.getenv('DB_PORT')
    db_string = f'postgres://{db_username}:{db_password}@{db_url}:{db_port}/postgres'

    db = create_engine(db_string, pool_size=100)
    return db


Base = automap_base()
engine = connect_to_db()
# reflect the tables
Base.prepare(engine, reflect=True)

# mapped classes are now created with names by default
# matching that of the table name.
SearchTerm = Base.classes.search_term
