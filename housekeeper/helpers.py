import os
import time

from contextlib import contextmanager
from textwrap import dedent
from datetime import timedelta, date

import structlog


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


def load_connection_config(filename=DBCONFIG):
    if not os.path.isfile(filename):
        raise SystemExit(f"No database config in {filename}")
    # File looks like
    """
    # DB settings\n
    DBHost=db1.modio.dcl1.synotio.net
    DBName=moodio.se
    DBUser=moodio.se
    DBPassword=sassabrassa booh
    DBPort=5432
    """

    dbhost = "localhost"
    dbname = "zabbix"
    dbuser = "zabbix"
    dbport = "5432"
    dbpass = ""

    with open(filename) as f:
        for line in f.readlines():
            if line.startswith("DBHost="):
                dbhost = line.split("=", 1)[1].strip()
            if line.startswith("DBName="):
                dbname = line.split("=", 1)[1].strip()
            if line.startswith("DBUser="):
                dbuser = line.split("=", 1)[1].strip()
            if line.startswith("DBPort="):
                dbport = line.split("=", 1)[1].strip()
            if line.startswith("DBPassword="):
                dbpass = line.split("=", 1)[1].strip()

    return (dbname, dbuser, dbhost, dbport, dbpass)


def connstring(filename=DBCONFIG):
    inputs = load_connection_config(filename=filename)
    output = (
        "dbname='%s' user='%s' host='%s' port='%s' password='%s'"
        " keepalives=1 keepalives_idle=15 keepalives_interval=15 keepalives_count=15"
    )
    return output % inputs


def archive_connstring(filename=DBCONFIG):
    dbname, dbuser, dbhost, dbport, dbpass = load_connection_config(filename=filename)
    output = (
        "host='%s' port='%s' dbname='%s' user='%s' password='%s' sslmode='%s'"
        " keepalives=1 keepalives_idle=15 keepalives_interval=15 keepalives_count=15"
    )
    dbname = os.environ.get("ARCHIVE_PGDATABASE", dbname)
    dbhost = os.environ.get("ARCHIVE_PGHOST", dbhost)
    dbport = os.environ.get("ARCHIVE_PGPORT", dbport)
    dbuser = os.environ.get("ARCHIVE_PGUSER", dbuser)
    dbpassword = os.environ.get("ARCHIVE_PGPASSWORD", dbpass)
    dbsslmode = os.environ.get("ARCHIVE_PGSSLMODE", "prefer")
    return output % (dbhost, dbport, dbname, dbuser, dbpassword, dbsslmode)


def get_table_name(table="history", year=2011, month=12):
    return f"{table}_y{year}m{month:02d}"


def get_index_name(table="history", year=2011, month=12, kind="btree"):
    tablename = get_table_name(table=table, year=year, month=month)
    return f"{tablename}_{kind}_idx"


def get_constraint_name(table="history", year=2011, month=12):
    return f"{table}_y{year}m{month:02d}_check"


def sql_if_tables_exist(tables, query_iter):
    count = len(tables)
    tables_string = ", ".join("'{}'".format(t) for t in tables)
    query_string = "\n".join(x for x in query_iter)
    yield dedent(
        f"""
        DO $$ BEGIN
        IF (SELECT COUNT(*)={count} FROM information_schema.tables WHERE table_name IN ({tables_string})) THEN
        {query_string}
        END IF; END $$;"""
    )


def table_exists(conn, table="history"):
    select = f"select count(*)=1 from pg_tables where tablename='{table}';"
    with conn.cursor() as c:
        c.execute(select)
        res = c.fetchone()
        return res[0]
