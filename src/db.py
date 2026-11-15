"""Thin database helper around psycopg2."""
import contextlib
import os
import psycopg2
import psycopg2.extras

from src.config import DB

# Cloud platforms (Render, Heroku, etc.) provide a single connection string
# in DATABASE_URL rather than individual host/port/user variables. When it's
# present, use it; otherwise fall back to the per-field config for local runs.
DATABASE_URL = os.environ.get("DATABASE_URL")


def connect(autocommit: bool = False):
    """Open a new connection."""
    if DATABASE_URL:
        conn = psycopg2.connect(DATABASE_URL)
    else:
        conn = psycopg2.connect(
            host=DB["host"], port=DB["port"], dbname=DB["dbname"],
            user=DB["user"], password=DB["password"],
        )
    conn.autocommit = autocommit
    return conn


@contextlib.contextmanager
def cursor(autocommit: bool = False, dict_rows: bool = False):
    """Context-managed cursor that commits/rolls back and closes cleanly."""
    conn = connect(autocommit=autocommit)
    factory = psycopg2.extras.RealDictCursor if dict_rows else None
    cur = conn.cursor(cursor_factory=factory)
    try:
        yield cur
        if not autocommit:
            conn.commit()
    except Exception:
        if not autocommit:
            conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()


def scalar(sql: str, params=None):
    """Run a query and return the first column of the first row."""
    with cursor() as cur:
        cur.execute(sql, params)
        row = cur.fetchone()
        return row[0] if row else None
