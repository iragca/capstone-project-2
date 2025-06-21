from pprint import pprint

from pathlib import Path
from src.config import INTERIM_DATA_DIR
import duckdb


class DB:

    def __init__(self, db_path: Path | str = INTERIM_DATA_DIR / "tweets.duckdb") -> None:
        self.db_path = db_path
        self.connection = duckdb.connect(self.db_path)

        self.init_table()

    def init_table(self) -> None:
        self.connection.execute(
            """
            CREATE TABLE IF NOT EXISTS tweets (
    id VARCHAR(30) PRIMARY KEY,
    text TEXT,
    retweet_count INTEGER,
    reply_count INTEGER,
    like_count INTEGER,
    quote_count INTEGER,
    community_note TEXT,
    comments TEXT,
    in_reply_to VARCHAR(30),
    sensitive_flag BOOLEAN,
    lang VARCHAR(10),
    time_of_day TIMESTAMP,
    is_reply BOOLEAN,
    source VARCHAR(255),
    url TEXT,
    author_id VARCHAR(30),
    author_name VARCHAR(255),
    verified_author BOOLEAN,
    bookmark_count INTEGER,
    views TEXT,
    has_moderated_replies BOOLEAN,
    community VARCHAR(255),
    timestamp_gathered TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
        """
        )

        self.execute(
            """CREATE TABLE IF NOT EXISTS errors (
        id TEXT,
        error_message TEXT,
        error_type TEXT,
        timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP                 
    )"""
        )

    def execute(self, query: str, params: tuple = ()) -> None:
        self.connection.execute(query, params)

    def fetchall(self, query: str, params: tuple = ()) -> list:
        return self.connection.execute(query, params).fetchall()

    def view_data(self) -> None:


        pprint(self.connection.execute(
            """
            SELECT * FROM errors
            """
        ).fetch_df())

        pprint(self.connection.execute(
            """
            SELECT * FROM tweets
            """
        ).fetch_df())


    def view_schema(self) -> None:
        schema = self.connection.execute("DESCRIBE tweets").fetch_df()
        pprint(schema)

    def close(self) -> None:
        self.connection.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def __del__(self):
        self.close()

    def __repr__(self):
        return f"DB(db_path={self.db_path})"

    def __str__(self):
        return f"Database connection to {self.db_path}"
