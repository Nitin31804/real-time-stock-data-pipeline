import os
import logging
import psycopg2
from psycopg2.pool import ThreadedConnectionPool
from psycopg2.extras import RealDictCursor

logger = logging.getLogger("dashboard.core.db")

# Connection Pool
db_pool = None


def get_pool():
    global db_pool
    if db_pool is None:
        logger.info("dY" " Initializing PostgreSQL Connection Pool...")
        db_pool = ThreadedConnectionPool(
            minconn=1,
            maxconn=10,
            host=os.getenv("POSTGRES_HOST", "postgres"),
            database=os.getenv("POSTGRES_DB", "stock_db"),
            user=os.getenv("POSTGRES_USER", "postgres"),
            password=os.getenv("POSTGRES_PASSWORD", "postgres"),
        )
    return db_pool


def get_db_connection():
    pool = get_pool()
    return pool.getconn()


def release_db_connection(conn):
    pool = get_pool()
    pool.putconn(conn)


def query_one(sql: str, params: tuple = ()):
    conn = get_db_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(sql, params)
            return cur.fetchone()
    finally:
        release_db_connection(conn)


def query_all(sql: str, params: tuple = ()):
    conn = get_db_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(sql, params)
            return cur.fetchall()
    finally:
        release_db_connection(conn)


def serialize_row(row):
    if not row:
        return row
    return {
        k: (v.isoformat() if hasattr(v, "isoformat") else v) for k, v in row.items()
    }
