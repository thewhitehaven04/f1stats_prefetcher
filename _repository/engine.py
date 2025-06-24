from asyncio.log import logger
from functools import cache
from os import environ
from sqlalchemy import Connection, create_engine


db_conn_string = (
    environ.get("DB_URL_PYTHON")
    or "postgresql://germanbulavkin:postgres@localhost:5432/postgres"
)
logger.info(f"--------\nDatabase connection string: {db_conn_string}\n---------")

postgres = create_engine(db_conn_string)

@cache
def get_connection() -> Connection:
    try:
        connection = postgres.connect()
        logger.info("Connection established")
        return connection
    except:
        raise ValueError("Database connection not initialized")
