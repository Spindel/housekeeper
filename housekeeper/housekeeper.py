#!/usr/bin/env python3

import psycopg2
import monthdelta
import pytz

from datetime import datetime

from .helpers import (
    connstring,
    get_start_and_stop,
    get_constraint_name,
    get_index_name,
    get_table_name,
    gen_current_and_future,
    gen_year_past,
    gen_last_month,
)


def gen_2014_to_2018():
    date = datetime(
            year=2014,
            month=1,
            day=1,
            hour=0,
            minute=0,
            second=0,
            tzinfo=pytz.utc)
    step = monthdelta.monthdelta(1)

    while date.year < 2019:
        yield date
        date = date + step


def clean_old_indexes(table="history", year=2011, month=12):
    tablename = get_table_name(table=table, year=year, month=month)
    oldindexes = [f"{tablename}_itemid_clock_idx",
                  f"{tablename}_itemid_clock_idx1",
                  f"{tablename}_itemid_clock_idx2"]
    cleanup = "DROP INDEX IF EXISTS {};"
    for oldindex in oldindexes:
        yield cleanup.format(oldindex)


def ensure_btree_index(table="history", year=2011, month=12):
    index = get_index_name(table=table, year=year, month=month, kind="btree")
    table = get_table_name(table=table, year=year, month=month)
    yield f"CREATE INDEX CONCURRENTLY IF NOT EXISTS {index} on {table} using btree (itemid, clock);"


def ensure_brin_index(table="history", year=2011, month=12):
    index = get_index_name(table=table, year=year, month=month, kind="brin")
    table = get_table_name(table=table, year=year, month=month)
    yield (f"CREATE INDEX CONCURRENTLY IF NOT EXISTS {index} on {table} "
           f"USING brin (itemid, clock) WITH (pages_per_range='16');")


def clean_btree_index(table="history", year=2011, month=12):
    index = get_index_name(table=table, year=year, month=month, kind="btree")
    yield f"DROP INDEX IF EXISTS {index};"


def clean_old_items(table="history", year=2011, month=12):
    table = get_table_name(table=table, year=year, month=month)
    yield f"DELETE FROM {table} WHERE itemid NOT IN (select itemid from items);"


def create_table_partition(table="history", year=2011, month=12):
    start, stop = get_start_and_stop(year=year, month=month)
    tablename = get_table_name(table=table, year=year, month=month)
    yield f"CREATE TABLE IF NOT EXISTS {tablename} PARTITION OF {table} for values from ({start}) to ({stop});"


def detach_partition(table="history", year=2011, month=12):
    tablename = get_table_name(table=table, year=year, month=month)
    detach = f"ALTER TABLE {table} DETACH PARTITION {tablename};"
    yield detach


def drop_check_constraint(table="history", year=2011, month=12):
    tablename = get_table_name(table=table, year=year, month=month)
    constraint_name = get_constraint_name(table=table, year=year, month=month)
    constraint = f"ALTER TABLE {tablename} DROP CONSTRAINT IF EXISTS {constraint_name};"
    yield constraint


def add_check_constraint(table="history", year=2011, month=12):
    tablename = get_table_name(table=table, year=year, month=month)
    constraint_name = get_constraint_name(table=table, year=year, month=month)
    start, stop = get_start_and_stop(year=year, month=month)
    constraint = (f"ALTER TABLE {tablename} ADD CONSTRAINT {constraint_name} "
                  f"CHECK (clock >= {start} AND clock < {stop});")
    yield from drop_check_constraint(table=table, year=year, month=month)
    yield constraint


def attach_partition(table="history", year=2011, month=12):
    start, stop = get_start_and_stop(year=year, month=month)
    partition_name = get_table_name(table=table, year=year, month=month)
    attach = f"ALTER TABLE {table} ATTACH PARTITION {partition_name} FOR VALUES FROM ({start}) TO ({stop});"
    yield attach


def cluster_table(table="history", year=2011, month=12):
    tablename = get_table_name(table=table, year=year, month=month)
    indexname = get_index_name(table=table, year=year, month=month, kind="btree")
    start, stop = get_start_and_stop(year=year, month=month)
    temp_table = f"{tablename}_temp"

    yield "BEGIN TRANSACTION;"
    yield from detach_partition(table=table, year=year, month=month)
    yield f"CREATE TABLE IF NOT EXISTS {temp_table} PARTITION OF {table} for values from ({start}) to ({stop});"
    yield "COMMIT;"

    yield "-- Create an b-tree index so we can cluster"
    yield from ensure_btree_index(table=table, year=year, month=month)
    yield f"CLUSTER {tablename} USING {indexname};"
    yield from add_check_constraint(table=table, year=year, month=month)
    yield from clean_btree_index(table=table, year=year, month=month)

    yield "-- Swap tables"
    yield "BEGIN TRANSACTION;"
    yield f"ALTER TABLE {table} DETACH PARTITION {temp_table};"
    yield from attach_partition(table=table, year=year, month=month)
    yield "COMMIT;"

    yield "-- Move any data that arrived while we were detached"
    yield "BEGIN TRANSACTION;"
    yield f"INSERT INTO {tablename} SELECT * from {temp_table} order by itemid,clock;"
    yield f"DROP TABLE {temp_table};"
    yield "COMMIT;"

    yield from drop_check_constraint(table=table, year=year, month=month)


def migrate_table(server, table="history", year=2011, month=12):
    """Code to migrate a table to a remote server."""
    raise NotImplemented("Not implemented yet")
    tablename = get_table_name(table=table, year=year, month=month)
    temp_table = f"{tablename}_temp"
    start, stop = get_start_and_stop(year=year, month=month)
    yield "BEGIN TRANSACTION;"
    yield from detach_partition(table=table, year=year, month=month)
    yield f"ALTER TABLE {tablename} RENAME TO {temp_table};"
    yield f"create foreign table if not exists {tablename} like {table} server {server};"
    yield from attach_partition(table=table, year=year, month=month)
    yield "COMMIT;"
    yield "BEGIN TRANSACTION;"
    yield f"INSERT INTO {tablename} SELECT * from {temp_table} order by itemid,clock;"
    yield f"DROP TABLE {temp_table};"
    yield f"ALTER TABLE {tablename} RENAME TO {tablename}_foreign;"
    yield "COMMIT;"


def do_maintenance(connstr, cluster=False):
    tables = ("history", "history_uint", "history_text", "history_str")

    with psycopg2.connect(connstr) as c:
        c.autocommit = True  # Don't implicitly open a transaction
        for date in gen_current_and_future():
            for table in tables:
                with c.cursor() as curs:
                    for x in create_table_partition(table=table, year=date.year, month=date.month):
                        curs.execute(x)

                with c.cursor() as curs:
                    for x in ensure_btree_index(table=table, year=date.year, month=date.month):
                        curs.execute(x)

                with c.cursor() as curs:
                    for x in clean_old_indexes(table=table, year=date.year, month=date.month):
                        curs.execute(x)

        for n, date in enumerate(gen_year_past()):
            previous_month = (n == 0)
            for table in tables:
                with c.cursor() as curs:
                    for x in clean_old_indexes(table=table, year=date.year, month=date.month):
                        curs.execute(x)

                with c.cursor() as curs:
                    for x in clean_old_items(table=table, year=date.year, month=date.month):
                        curs.execute(x)

                if not previous_month:
                    with c.cursor() as curs:
                        for x in ensure_brin_index(table=table, year=date.year, month=date.month):
                            curs.execute(x)

                    with c.cursor() as curs:
                        for x in clean_btree_index(table=table, year=date.year, month=date.month):
                            curs.execute(x)

        if cluster:
            for date in gen_last_month():
                for table in tables:
                    with c.cursor() as curs:
                        for x in cluster_table(table=table, year=date.year, month=date.month):
                            curs.execute(x)


def oneshot_maintenance():
    tables = ("history", "history_uint", "history_text", "history_str")
    for date in gen_2014_to_2018():
        for table in tables:
            yield from ensure_brin_index(table=table, year=date.year, month=date.month)
            yield from clean_old_indexes(table=table, year=date.year, month=date.month)
            yield from clean_old_items(table=table, year=date.year, month=date.month)
            yield from cluster_table(table=table, year=date.year, month=date.month)


def main():
    connstr = connstring()
    should_cluster = datetime.utcnow().day == 14
    do_maintenance(connstr=connstr, cluster=should_cluster)


if __name__ == '__main__':
    main()
