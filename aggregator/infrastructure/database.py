from contextlib import contextmanager
from pathlib import Path
from typing import Iterator

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from aggregator.settings import settings

_engine: Engine | None = None


def get_engine() -> Engine:
    """Provide a singleton SQLAlchemy engine."""
    global _engine
    if _engine is None:
        db = settings.database
        _engine = create_engine(
            f"mysql+mysqlconnector://{db.user}:{db.password}@{db.host}/{db.name}"
        )
    return _engine


@contextmanager
def connection() -> Iterator:
    engine = get_engine()
    conn = engine.connect()
    try:
        yield conn
        conn.commit()
    finally:
        conn.close()


def execute_sql_file(filepath: str) -> None:
    """Execute a SQL file against the configured database."""
    path = Path(filepath)
    if not path.exists():
        raise FileNotFoundError(f"SQL file not found: {filepath}")

    with connection() as conn:
        sql_commands = path.read_text().split(";")
        for command in sql_commands:
            cleaned = command.strip()
            if not cleaned:
                continue
            conn.execute(text(cleaned))
