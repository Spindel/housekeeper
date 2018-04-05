#!/usr/bin/env python3
import os
import sys
import itertools
import psycopg2

from .helpers import (
    archive_connstring,
    connstring,
    execute,
    get_start_and_stop,
    get_month_before_retention,
    get_table_name,
    gen_current_and_future,
    gen_year_past,
    gen_between,
    gen_2014_to_current,
)

from .housekeeper import (
    ensure_brin_index,
)

CREATE_ROOT = {
    "history_str": """CREATE TABLE IF NOT EXISTS {tablename} (
                          itemid bigint NOT NULL,
                          clock integer NOT NULL CHECK (clock >= {start} and clock <{stop}),
                          value character varying(255) NOT NULL,
                          ns integer NOT NULL);""",

    "history": """CREATE TABLE IF NOT EXISTS {tablename} (
                          itemid bigint NOT NULL,
                          clock integer NOT NULL CHECK (clock >= {start} and clock <{stop}),
                          value numeric(16,4) NOT NULL,
                          ns integer NOT NULL);""",

    "history_text": """CREATE TABLE IF NOT EXISTS {tablename} (
                          id bigint NOT NULL,
                          itemid bigint NOT NULL,
                          clock integer NOT NULL CHECK (clock >= {start} and clock <{stop}),
                          value text NOT NULL,
                          ns integer NOT NULL
                    );""",

    "history_uint": """CREATE TABLE IF NOT EXISTS {tablename} (
                          itemid bigint NOT NULL,
                          clock integer NOT NULL CHECK (clock >= {start} and clock <{stop}),
                          value numeric(20,0) NOT NULL,
                          ns integer NOT NULL);""",
}

FOREIGN_NAMES = {
    "history": "archive",
    "history_str": "archive_str",
    "history_text": "archive_text",
    "history_uint": "archive_uint",
}


def get_archive():
    """
    environment variable MODIO_ARCHIVE in days is used to decide on how many
    days old data should be before being moved into the ARCHIVE"""
    retention = os.environ.get("MODIO_ARCHIVE")
    if retention is None:
        print("Set environment variable MODIO_ARCHIVE to amount of days to handle")
        raise SystemExit
    retention = int(retention)
    return retention


def archive_setup(username="moodio.se", password="0000-0000-0000-0000"):
    initial_setup = f"""
CREATE ROLE "{username}" PASSWORD '{password}' NOSUPERUSER NOCREATEDB NOCREATEROLE INHERIT LOGIN;
CREATE DATABASE "{username}" OWNER "{username}";
"""
    print(initial_setup)
    as_postgres = f'SET ROLE "{username}";'
    print(as_postgres)
    for date in gen_2014_to_current():
        for table in FOREIGN_NAMES:
            for x in create_archive_table(table=table, year=date.year, month=date.month):
                print(x)


def migrate_setup(username="moodio.se", password="0000-0000-0000-0000"):
    as_postgres = f"""
CREATE EXTENSION IF NOT EXISTS postgres_fdw;
CREATE SERVER IF NOT EXISTS archive FOREIGN DATA WRAPPER postgres_fdw OPTIONS (host 'db2.modio.dcl1.synotio.net' , port '5432', dbname '{username}', sslmode 'verify-full');
CREATE USER MAPPING IF NOT EXISTS FOR "{username}" SERVER archive OPTIONS (user '{username}', password '{password}');
CREATE USER MAPPING IF NOT EXISTS FOR "admin.{username}" SERVER archive OPTIONS (user '{username}', password '{password}');
GRANT ALL ON FOREIGN SERVER "archive" to "{username}";
SET ROLE "{username}";
"""
    print(as_postgres)


def create_archive_table(table="history", year=2011, month=12):
    tname = FOREIGN_NAMES[table]
    create = CREATE_ROOT[table]
    tablename = get_table_name(table=tname, year=year, month=month)
    start, stop = get_start_and_stop(year=year, month=month)
    yield create.format(tablename=tablename, start=start, stop=stop)
    yield from ensure_brin_index(table=tname, year=year, month=month)


def detach_partition(table="history", year=2011, month=12):
    tablename = get_table_name(table=table, year=year, month=month)
    yield f"ALTER TABLE {table} DETACH PARTITION {tablename};"


def create_foreign_table(table="history", year=2011, month=12, remote="archive"):
    tname = FOREIGN_NAMES[table]
    tablename = get_table_name(table=tname, year=year, month=month)
    start, stop = get_start_and_stop(year=year, month=month)
    yield f"CREATE FOREIGN TABLE IF NOT EXISTS {tablename} PARTITION OF {table} FOR VALUES FROM ({start}) TO ({stop}) SERVER {remote};"


def sql_if_tables_exists(tables, query_iter):
    count = len(tables)
    tables_string = ", ".join("'{}'".format(t) for t in tables)
    yield "DO $$ BEGIN"
    yield f"""IF ( SELECT COUNT(*)={count} FROM information_schema.tables WHERE table_name IN ({tables_string})) THEN"""
    yield from query_iter
    yield """END IF; END $$;"""
    yield ""


def migrate_table_to_archive(table="history", year=2011, month=12):
    tname = FOREIGN_NAMES[table]
    original_tablename = get_table_name(table=table, year=year, month=month)
    remote_tablename = get_table_name(table=tname, year=year, month=month)

    start, stop = get_start_and_stop(year=year, month=month)

    query_iter = itertools.chain(
        detach_partition(table=table, year=year, month=month),
        create_foreign_table(table=table, year=year, month=month),
    )
    yield "BEGIN TRANSACTION;"
    yield from sql_if_tables_exists(tables=[original_tablename], query_iter=query_iter)
    yield "COMMIT;"

    tables = (remote_tablename, original_tablename)

    def query_iter():
        yield f"""WITH moved_rows as ( DELETE FROM {original_tablename} a RETURNING a.* ) INSERT INTO {remote_tablename} SELECT * from moved_rows order by itemid, clock;"""
        yield f"DROP TABLE IF EXISTS {original_tablename};"

    yield from sql_if_tables_exists(tables=tables, query_iter=query_iter())
    yield ""


def archive_maintenance(connstr, cluster=False):
    tables = ("history", "history_uint", "history_text", "history_str")

    with psycopg2.connect(connstr) as c:
        c.autocommit = True  # Don't implicitly open a transaction
        for table in tables:
            for date in gen_current_and_future():
                with c.cursor() as curs:
                    for x in create_archive_table(table=table, year=date.year, month=date.month):
                        execute(curs, x)
            for date in gen_year_past():
                with c.cursor() as curs:
                    for x in create_archive_table(table=table, year=date.year, month=date.month):
                        execute(curs, x)


def migrate_data(connstr):
    tables = ("history", "history_uint", "history_text", "history_str")

    retention = get_archive()
    end = get_month_before_retention(retention=retention)

    with psycopg2.connect(connstr) as c:
        c.autocommit = True  # Don't implicitly open a transaction
        for date in gen_between(to_date=end):
            for table in tables:
                with c.cursor() as curs:
                    for x in migrate_table_to_archive(table=table, year=date.year, month=date.month):
                        execute(curs, x)


def oneshot_archive():
    tables = ("history", "history_uint", "history_text", "history_str")
    retention = get_archive()
    end = get_month_before_retention(retention=retention)

    for date in gen_between(to_date=end):
        for table in tables:
            for x in create_archive_table(table=table, year=date.year, month=date.month):
                print(x)


def oneshot_migrate():
    tables = ("history", "history_uint", "history_text", "history_str")
    retention = get_archive()
    end = get_month_before_retention(retention=retention)

    for date in gen_between(to_date=end):
        for table in tables:
            for x in migrate_table_to_archive(table=table, year=date.year, month=date.month):
                print(x)


def main():
    if len(sys.argv) != 2:
        print("{}: command, (setup_archive, setup_migrate, oneshot_archive, oneshot_migrate, cron)".format(sys.argv[0]))
        print("setup commands are to be run first on either system.")
        print("Archive commands are to prepare the archive server")
        sys.exit(1)
    command = sys.argv[1]

    if command == "setup_archive":
        archive_setup()
    elif command == "setup_migrate":
        migrate_setup()
    elif command == "oneshot_archive":
        oneshot_archive()
    elif command == "oneshot_migrate":
        oneshot_migrate()
    elif command == "cron":
        connstr = archive_connstring()
        archive_maintenance(connstr=connstr)
        connstr = connstring()
        migrate_data(connstr=connstr)


if __name__ == '__main__':
    main()
