#!/usr/bin/env python3
import os
import sys
import itertools
import threading
import time
import psycopg2

import structlog

from textwrap import dedent

from .times import (
    get_start_and_stop,
    get_month_before_retention,
    months_for_year_ahead,
    months_for_year_past,
    months_between,
)

from .helpers import (
    get_table_name,
    archive_connstring,
    connstring,
    execute,
    sql_if_tables_exist,
    table_exists,
    prelude_cursor,
    log_and_reset_notices,
)
from .logs import setup_logging, log_state

from .housekeeper import (
    ensure_brin_index,
    do_cluster_operation,
    clean_duplicate_items,
    clean_old_items,
    clean_expired_items,
    should_maintain,
)

CREATE_ROOT = {
    "history_str": """CREATE TABLE IF NOT EXISTS {tablename} (
                          itemid BIGINT NOT NULL,
                          clock INTEGER NOT NULL CHECK (clock >= {start} AND clock < {stop}),
                          value CHARACTER VARYING(255) NOT NULL,
                          ns INTEGER NOT NULL);""",

    "history": """CREATE TABLE IF NOT EXISTS {tablename} (
                          itemid BIGINT NOT NULL,
                          clock INTEGER NOT NULL CHECK (clock >= {start} AND clock < {stop}),
                          value NUMERIC(16,4) NOT NULL,
                          ns INTEGER NOT NULL);""",

    "history_text": """CREATE TABLE IF NOT EXISTS {tablename} (
                          id BIGINT NOT NULL,
                          itemid BIGINT NOT NULL,
                          clock INTEGER NOT NULL CHECK (clock >= {start} AND clock < {stop}),
                          value TEXT NOT NULL,
                          ns INTEGER NOT NULL
                    );""",

    "history_uint": """CREATE TABLE IF NOT EXISTS {tablename} (
                          itemid BIGINT NOT NULL,
                          clock INTEGER NOT NULL CHECK (clock >= {start} AND clock < {stop}),
                          value NUMERIC(20,0) NOT NULL,
                          ns INTEGER NOT NULL);""",
}

FOREIGN_NAMES = {
    "history": "archive",
    "history_str": "archive_str",
    "history_text": "archive_text",
    "history_uint": "archive_uint",
}


_log = structlog.get_logger()


def get_retention():
    """
    environment variable MODIO_ARCHIVE in days is used to decide on how many
    days old data should be before being moved into the ARCHIVE"""
    retention = os.environ.get("MODIO_ARCHIVE")
    if retention is None:
        print("Set environment variable MODIO_ARCHIVE to amount of days to handle")
        raise SystemExit
    retention = int(retention)
    return retention


def archive_setup(username="example.com", password="0000-0000-0000-0000"):
    username = os.getenv("ARCHIVE_PGUSER", username)
    password = os.getenv("ARCHIVE_PGPASSWORD", password)

    initial_setup = f"""
        CREATE ROLE "{username}" PASSWORD '{password}' NOSUPERUSER NOCREATEDB NOCREATEROLE INHERIT LOGIN;
        CREATE DATABASE "{username}" OWNER "{username}";
        """
    print(dedent(initial_setup))
    as_postgres = f'SET ROLE "{username}";'
    print(as_postgres)


def migrate_setup(username="example.com", password="0000-0000-0000-0000", host='db2.example.com'):
    host = os.getenv("ARCHIVE_PGHOST", host)
    username = os.getenv("ARCHIVE_PGUSER", username)
    password = os.getenv("ARCHIVE_PGPASSWORD", password)
    port = os.getenv("ARCHIVE_DBPORT", "5432")
    sslmode = os.getenv("ARCHIVE_SSLMODE", "verify-full")

    as_postgres = f"""
        CREATE EXTENSION IF NOT EXISTS postgres_fdw;
        CREATE SERVER IF NOT EXISTS archive
        FOREIGN DATA WRAPPER postgres_fdw
            OPTIONS (host '{host}', port '{port}', dbname '{username}', sslmode '{sslmode}', fetch_size '10000');
        CREATE USER MAPPING IF NOT EXISTS FOR "{username}"
            SERVER archive
            OPTIONS (user '{username}', password '{password}');
        CREATE USER MAPPING IF NOT EXISTS FOR "admin.{username}"
            SERVER archive
            OPTIONS (user '{username}', password '{password}');
        GRANT ALL ON FOREIGN SERVER "archive" to "{username}";
        SET ROLE "{username}";"""
    print(dedent(as_postgres))


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
    yield dedent(f"""
        CREATE FOREIGN TABLE IF NOT EXISTS {tablename}
        PARTITION OF {table} FOR VALUES FROM ({start}) TO ({stop}) SERVER {remote};""")


def archive_cluster(table="history", year=2011, month=12):
    """Cluster an archive table. Assumes the table exists"""
    arname = FOREIGN_NAMES[table]
    yield from do_cluster_operation(table=arname, year=year, month=month)


def archive_dedupe(table="history", year=2011, month=12):
    """Cluster an archive table. Assumes the table exists"""
    arname = FOREIGN_NAMES[table]
    yield from clean_duplicate_items(table=arname, year=year, month=month)


def should_archive_cluster(conn, table="history", year=2011, month=12):
    """Cluster an archive table. Requires a connection to test if the table
    exists"""
    arname = FOREIGN_NAMES[table]
    tablename = get_table_name(table=arname, year=year, month=month)
    return table_exists(conn, table=tablename)


def python_migrate_table_to_archive(src_conn, dst_conn, table="history", year=2011, month=12):
    """This uses python code and threads to transfer data between tables.
    While the method is generic, there cannot be a transaction for COPY
    operations (other than read data) and we cannot verify the data exists in
    the target before deleting it.

    Since we use partitions, we know there won't be writing to our source table
    while this code runs, so it's "safe" to forego using transactions.

    This code also assumes that if nothing breaks during the two COPY jobs,
    it's okay to truncate the source table.
    """

    arname = FOREIGN_NAMES[table]
    src_table = get_table_name(table=table, year=year, month=month)
    dst_table = get_table_name(table=arname, year=year, month=month)

    # option 1, create sorted temp table.
    # * How do I remove data from orig table once transfer is done?
    # * How do I know that I'm not deleting data that arrived in between
    #     I created temp table and done?

    # option 2, Ignore order, hope it's correct, just copy & delete.
    # * using copy ( delete * from {} del returning del.*) TO STDOUT
    # * this loses _all_ data from source in case error happens on remote.

    # option 3, try to delete & sort in order without creating a giant buffer
    # * delete from where ctid in (select ...)  fails because it sorts by ctid
    #  with delete... as ; fails because it's not valid for COPY

    # FOR FUCKS SAKE SQL, WHY DO YOU MAKE MY LIFE PAINFUL?

    # And of course, it turns out that even _if_ you order on insert, it
    # doesn't _store_ things in order, so you need to cluster the table
    # _anyhow_

    if not table_exists(conn=src_conn, table=src_table):
        return
    if not table_exists(conn=dst_conn, table=dst_table):
        return
    src_query = f"COPY (SELECT * FROM {src_table}) TO STDOUT;"

    log = _log.bind(source=src_table, destination=dst_table)
    log.info("Tables exist. Starting data transfer")

    # a pipe to read/write with, will be wrapped in file handles for the sake
    # of the API
    readEnd, writeEnd = os.pipe()
    threadlog = log.getChild("copy_from")
    copy_to_failed = copy_from_failed = True

    def copy_from():
        """Internal function for the thread"""
        nonlocal copy_from_failed
        try:
            start = time.monotonic()
            sql_file = os.fdopen(readEnd)
            with dst_conn.cursor() as c:
                # Copy has no return value, we cannot see how many rows were
                # transferred
                c.copy_from(sql_file, dst_table)
            dst_conn.commit()
            elapsed = time.monotonic() - start
        except Exception:
            threadlog.exception("Something is wrong in the state of denmark")
        else:
            copy_from_failed = False
            threadlog.info("Spent %ss writing to %s", elapsed, dst_table)
        finally:
            sql_file.close()

    archive_side = threading.Thread(target=copy_from)
    archive_side.start()

    # Explicit write flag on the write side.
    sql_file = os.fdopen(writeEnd, 'w')
    start = time.monotonic()

    # If the below fails (disk full, similar) we cannot terminate the
    # copy_from thread, and will thus leak a db connection until the program
    # terminates
    try:
        with src_conn.cursor() as c:
            c.copy_expert(src_query, sql_file)
            sql_file.close()  # Important, otherwise you deadlock
            elapsed = time.monotonic() - start
            log.info("Spent %ss reading from %s", elapsed, src_table)
    except Exception:
        log.exception("Error reading from table")
    else:
        copy_to_failed = False
    finally:
        sql_file.close()

    # The read is done, the receiving thread may take longer time to work it
    # out. Wait for it.
    try:
        archive_side.join()
    except Exception:
        log.error("Trouble receiving data for some reason. Not cleaning up")
        return

    if copy_from_failed or copy_to_failed:
        return

    # We now have a clean copy of the detached table. Time to empty it
    with src_conn.cursor() as c:
        execute(c, f"TRUNCATE TABLE {src_table};")

    # And then we need to cluster the table on the archive side to
    # get it in-order
    with dst_conn.cursor() as curs:
        for x in archive_cluster(table=table, year=year, month=month):
            execute(curs, x)


def swap_live_and_archive_tables(table="history", year=2011, month=12):
    original_tablename = get_table_name(table=table, year=year, month=month)

    query_iter = itertools.chain(
        detach_partition(table=table, year=year, month=month),
        create_foreign_table(table=table, year=year, month=month),
    )
    yield from sql_if_tables_exist(tables=[original_tablename], query_iter=query_iter)


def migrate_table_to_archive(table="history", year=2011, month=12):
    tname = FOREIGN_NAMES[table]
    original_tablename = get_table_name(table=table, year=year, month=month)
    remote_tablename = get_table_name(table=tname, year=year, month=month)

    tables = (remote_tablename, original_tablename)

    def query_iter():
        yield dedent(f"""
            WITH moved_rows AS (DELETE FROM {original_tablename} a RETURNING a.*)
                INSERT INTO {remote_tablename} SELECT * FROM moved_rows ORDER BY itemid, clock;""")
        yield f"DROP TABLE IF EXISTS {original_tablename};"

    yield from sql_if_tables_exist(tables=tables, query_iter=query_iter())


def archive_maintenance(connstr):
    tables = ("history", "history_uint", "history_text", "history_str")

    with psycopg2.connect(connstr) as c:
        c.autocommit = True  # Don't implicitly open a transaction
        with log_state(stage="archive_maintenance"):
            for date in months_for_year_ahead():
                for table in tables:
                    for x in create_archive_table(table=table, year=date.year, month=date.month):
                        with prelude_cursor(c) as curs:
                            execute(curs, x)

            for date in months_for_year_past():
                for table in tables:
                    for x in create_archive_table(table=table, year=date.year, month=date.month):
                        with prelude_cursor(c) as curs:
                            execute(curs, x)


def migrate_data(source_connstr, dest_connstr):
    tables = ("history",  "history_uint", "history_text", "history_str")

    retention = get_retention()
    end = get_month_before_retention(retention=retention)

    with psycopg2.connect(source_connstr) as source, psycopg2.connect(dest_connstr) as dest:
        source.autocommit = True  # Don't implicitly open a transaction
        dest.autocommit = True

        for conn in (source, dest):
            with prelude_cursor(conn) as curs:
                execute(curs, "SELECT 1;")

        for date in months_between(to_date=end):
            for table in tables:
                # Should_maintain checks that the table exists first
                if should_maintain(conn=source, table=table, year=date.year, month=date.month):
                    # First clean up old (deleted) items
                    for x in clean_old_items(table=table, year=date.year, month=date.month):
                        with prelude_cursor(source) as curs:
                            execute(curs, x)
                    # Then clean out expired items (should be deleted)
                    for x in clean_expired_items(table=table, year=date.year, month=date.month, retention=retention):
                        with prelude_cursor(source) as curs:
                            execute(curs, x)

                    # Then clean up duplicate data ( warning, slow)
                    for x in clean_duplicate_items(table=table, year=date.year, month=date.month):
                        with prelude_cursor(source) as curs:
                            execute(curs, x)

                with prelude_cursor(source) as curs:
                    for x in swap_live_and_archive_tables(table=table, year=date.year, month=date.month):
                        try:
                            execute(curs, x)
                        except psycopg2.ProgrammingError:
                            pass
                # First we do the high performance COPY operation
                python_migrate_table_to_archive(src_conn=source, dst_conn=dest,
                                                table=table, year=date.year, month=date.month)
                log_and_reset_notices(conn=source)
                log_and_reset_notices(conn=dest)
                # Then we do the slow performance one that also cleans out the
                # tables.
                with prelude_cursor(source) as curs:
                    for x in migrate_table_to_archive(table=table, year=date.year, month=date.month):
                        execute(curs, x)


def oneshot_cluster(connstr):
    tables = ("history", "history_uint", "history_text", "history_str")
    retention = get_retention()
    end = get_month_before_retention(retention=retention)

    with psycopg2.connect(connstr) as conn:
        conn.autocommit = True  # Don't implicitly open a transaction
        for date in months_between(to_date=end):
            for table in tables:
                if should_archive_cluster(conn, table=table, year=date.year, month=date.month):
                    for x in archive_cluster(table=table, year=date.year, month=date.month):
                        with prelude_cursor(conn) as curs:
                            execute(curs, x)


def oneshot_dedupe(connstr):
    tables = ("history", "history_uint", "history_text", "history_str")
    retention = get_retention()
    end = get_month_before_retention(retention=retention)

    with psycopg2.connect(connstr) as conn:
        conn.autocommit = True  # Don't implicitly open a transaction
        for date in months_between(to_date=end):
            for table in tables:
                if should_archive_cluster(conn, table=table, year=date.year, month=date.month):
                    for x in archive_dedupe(table=table, year=date.year, month=date.month):
                        with prelude_cursor(conn) as curs:
                            execute(curs, x)


def oneshot_archive(connstr):
    tables = ("history", "history_uint", "history_text", "history_str")
    retention = get_retention()
    end = get_month_before_retention(retention=retention)

    with psycopg2.connect(connstr) as conn:
        conn.autocommit = True  # Don't implicitly open a transaction

        for date in months_between(to_date=end):
            for table in tables:
                for x in create_archive_table(table=table, year=date.year, month=date.month):
                    with prelude_cursor(conn) as curs:
                        execute(curs, x)


def oneshot_migrate():
    tables = ("history", "history_uint", "history_text", "history_str")
    retention = get_retention()
    end = get_month_before_retention(retention=retention)

    for date in months_between(to_date=end):
        for table in tables:
            for x in migrate_table_to_archive(table=table, year=date.year, month=date.month):
                print(x)


def main():
    setup_logging()

    if len(sys.argv) != 2:
        print(f"Usage: {sys.argv[0]} {{ COMMAND }}")
        print("where COMMAND := { setup_archive | setup_migrate | oneshot_archive | oneshot_cluster | cron | dedupe }")
        print()
        print("Setup commands are to be run first on either system.")
        print("oneshot_archive sets up the archive tables on the archive server")
        print("oneshot_cluster:  Cluster all tables on the archive server")
        print("cron: Creates archive tables on the the archive db for the past"
              "year, and the year ahead.")
        print("Then it migrates all tables older than MODIO_ARCHIVE days from"
              "the source to the archive db, finally cleaning out the old tables")
        print("dedupe: Iterates over all tables, removing duplicated rows.")
        sys.exit(1)
    command = sys.argv[1]

    if command == "setup_archive":
        archive_setup()
    elif command == "setup_migrate":
        migrate_setup()
    elif command == "oneshot_archive":
        archive_connstr = archive_connstring()
        oneshot_archive(archive_connstr)
    elif command == "oneshot_cluster":
        archive_connstr = archive_connstring()
        oneshot_cluster(archive_connstr)
    elif command == "dedupe":
        archive_connstr = archive_connstring()
        oneshot_dedupe(archive_connstr)
    elif command == "cron":
        archive_connstr = archive_connstring()
        archive_maintenance(connstr=archive_connstr)
        source_connstr = connstring()
        migrate_data(source_connstr=source_connstr,
                     dest_connstr=archive_connstr)
    print("/* All operations succesful! */")


if __name__ == '__main__':
    main()
