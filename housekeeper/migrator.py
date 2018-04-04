#!/usr/bin/env python3

import psycopg2

from .helpers import (
    connstring,
    execute,
    get_start_and_stop,
    get_table_name,
    gen_year_past,
)

from .housekeeper import (
    gen_2014_to_2018,
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


def original_setup(username="moodio.se", password="0000-0000-0000-0000"):
    as_postgres = f"""
CREATE EXTENSION if not exists postgres_fdw;
CREATE SERVER archive FOREIGN DATA WRAPPER postgres_fdw OPTIONS (host 'db2.modio.dcl1.synotio.net' , port '5432', dbname '{username}');
CREATE USER MAPPING FOR "{username}" SERVER archive OPTIONS (user '{username}', password '{password}', sslmode='verify-full' );
CREATE USER MAPPING FOR "admin.{username}" SERVER archive OPTIONS (user '{username}', password '{password}', sslmode='verify-full');
GRANT ALL ON FOREIGN SERVER "archive" to  "{username}";
"""
    print(as_postgres)
    for date in gen_2014_to_2018():
        for table in FOREIGN_NAMES:
            for x in create_remote_table(table=table, year=date.year, month=date.month):
                print(x)


def create_remote_table(table="history", year=2011, month=12):
    tname = FOREIGN_NAMES[table]
    create = CREATE_ROOT[table]
    tablename = get_table_name(table=table, year=year, month=month)
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


def migrate_table_to_foreign(table="history", year=2011, month=12):
    tname = FOREIGN_NAMES[table]
    original_tablename = get_table_name(table=table, year=year, month=month)
    remote_tablename = get_table_name(table=tname, year=year, month=month)

    start, stop = get_start_and_stop(year=year, month=month)
    yield "BEGIN TRANSACTION;"
    yield from detach_partition(table=table, year=year, month=month)
    yield from create_foreign_table(table=table, year=year, month=month)
    yield "COMMIT;"

    # SQL. Why are you so fucking awkward?
    yield f"""WITH moved_rows as (
                DELETE FROM {original_tablename} a RETURNING a.*
              )
              INSERT INTO {remote_tablename} SELECT * from moved_rows order by itemid, clock;"""
    yield f"DROP TABLE IF EXISTS {original_tablename};"


def remote_maintenance(connstr, cluster=False):
    tables = ("history", "history_uint", "history_text", "history_str")

    with psycopg2.connect(connstr) as c:
        c.autocommit = True  # Don't implicitly open a transaction
        for date in gen_year_past():
            for table in tables:
                with c.cursor() as curs:
                    for x in create_remote_table(table=table, year=date.year, month=date.month):
                        execute(curs, x)


def main():
    connstr = connstring()
    remote_maintenance(connstr=connstr)


if __name__ == '__main__':
    original_setup()
    main()
