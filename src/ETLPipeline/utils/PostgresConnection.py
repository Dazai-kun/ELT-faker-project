from contextlib import contextmanager
import psycopg2
from typing import Iterator

class PostgresDatabaseConnection:
    def __init__(self, host: str, port: str, user: str, password: str, database: str):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database

    @contextmanager
    def connect(self) -> Iterator[psycopg2.extensions.cursor]:
        try:
            conn = psycopg2.connect(
                dbname=self.database,
                user=self.user,
                password=self.password,
                host=self.host,
                port=self.port
            )
            cur = conn.cursor()
            yield cur
        finally:
            conn.commit()
            cur.close()
            conn.close()