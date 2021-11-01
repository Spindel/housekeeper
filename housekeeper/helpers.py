import os
import time

from contextlib import contextmanager
from datetime import timedelta, date

import structlog
import psycopg2


from .logs import log_state

_log = structlog.get_logger(__name__)


DBCONFIG = "/etc/zabbix/zabbix.conf.d/database.conf"
EPOCH = date(1970, 1, 1)
MONTHISH = timedelta(days=31)


def get_role():
    """Return a suitable name for the SET ROLE operation."""
    rolename = os.environ.get("HOUSEKEEPER_ROLE", "")
    rolename = rolename.strip()
    if not rolename:
        raise ValueError("Environment variable HOUSEKEEPER_ROLE must be set.")
    return rolename


def sql_prelude():
    with log_state(step="sql_prelude"):
        role = get_role()
        yield f"""SET ROLE "{role}";"""
        yield "SET WORK_MEM='1GB';"


def log_and_reset_notices(conn):
    """Log the notifications."""
    log = _log.bind(
        dbhost=conn.info.host, dbname=conn.info.dbname, dbuser=conn.info.user
    )
    if not conn.notices:
        return

    for n, msg in enumerate(conn.notices):
        log.info("DB notice", notice_no=n, notice=msg)
    conn.notices.clear()


@contextmanager
def connect_autocommit(connstr: str):
    """Yield a psycopg2 connection in autocommit mode.

    connstr is the same argument as psycopg2.connect.
    """
    conn = None
    try:
        conn = psycopg2.connect(connstr)
        conn.set_session(autocommit=True)  # Don't implicitly open a transaction
        yield conn
    finally:
        if conn:
            conn.close()


@contextmanager
def prelude_cursor(conn):
    with conn.cursor() as curs:
        for prelude in sql_prelude():
            execute(curs, prelude)
        yield curs
    log_and_reset_notices(conn)


def execute(cursor, query):
    info = cursor.connection.info
    log = _log.bind(dbhost=info.host, dbname=info.dbname, dbuser=info.user, query=query)

    start = time.monotonic()
    log.info("executing")
    result = cursor.execute(query)
    end = time.monotonic()
    elapsed = end - start
    log.info("Done", result=result, elapsed=f"{elapsed:06.2f}")
    return result


def housekeeper_connstring():
    return env_connstring(prefix="HOUSEKEEPER")


def archive_connstring():
    return env_connstring(prefix="ARCHIVE")


def env_connstring(filename=DBCONFIG, prefix="MOOMIN"):
    """Load a connstring from various places"""
    dbsslmode = os.environ.get(f"{prefix}_PGSSLMODE", "prefer")
    try:
        dbname = os.environ[f"{prefix}_PGDATABASE"]
        dbhost = os.environ[f"{prefix}_PGHOST"]
        dbport = os.environ[f"{prefix}_PGPORT"]
        dbuser = os.environ[f"{prefix}_PGUSER"]
        dbpass = os.environ[f"{prefix}_PGPASSWORD"]
    except KeyError as ex:
        msg = str(ex)
        raise SystemExit(f"Missing env var {msg}") from ex

    output = (
        f"host='{dbhost}' port='{dbport}' dbname='{dbname}' user='{dbuser}' password='{dbpass}' sslmode='{dbsslmode}'"
        f" keepalives=1 keepalives_idle=15 keepalives_interval=15 keepalives_count=15"
    )
    return output


def get_table_name(table="history", year=2011, month=12):
    return f"{table}_y{year}m{month:02d}"


def get_index_name(table="history", year=2011, month=12, kind="btree"):
    tablename = get_table_name(table=table, year=year, month=month)
    return f"{tablename}_{kind}_idx"


def get_constraint_name(table="history", year=2011, month=12):
    return f"{table}_y{year}m{month:02d}_check"


def table_exists(conn, table="history"):
    select = f"select count(*)=1 from pg_tables where tablename='{table}';"
    with conn.cursor() as c:
        c.execute(select)
        res = c.fetchone()
        return res[0]
