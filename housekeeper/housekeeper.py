#!/usr/bin/env python3

import psycopg2
import monthdelta
import pytz

from datetime import datetime

from .helpers import (
    connstring,
    get_index_name,
    get_table_name,
)


def gen_current_and_future(date=None):
    if date is None:
        date = datetime.utcnow()

    start = datetime(
            year=date.year,
            month=date.month,
            day=1,
            hour=0,
            minute=0,
            second=0,
            tzinfo=pytz.utc)
    step = monthdelta.monthdelta(1)
    yield start
    for x in range(12):
        start = start + step
        yield start


def gen_year_past():
    start = datetime.utcnow()
    step = monthdelta.monthdelta(13)
    start = start - step
    yield from gen_current_and_future(date=start)


def clean_old_indexes(table="history", year=2011, month=12):
    tablename = get_table_name(table=table, year=year, month=month)
    oldindexes = [f"{tablename}_itemid_clock_idx",
                  f"{tablename}_itemid_clock_idx1",
                  f"{tablename}_itemid_clock_idx2"]
    cleanup = "DROP INDEX IF EXISTS {};"
    for oldindex in oldindexes:
        yield cleanup.format(oldindex)


def create_btree_index(table="history", year=2011, month=12):
    index = get_index_name(table=table, year=year, month=month, kind="btree")
    table = get_table_name(table=table, year=year, month=month)
    yield f"CREATE INDEX IF NOT EXISTS {index} on {table} using btree (itemid, clock);"


def create_brin_index(table="history", year=2011, month=12):
    index = get_index_name(table=table, year=year, month=month, kind="brin")
    table = get_table_name(table=table, year=year, month=month)
    yield f"CREATE INDEX IF NOT EXISTS {index} on {table} using brin (itemid, clock) WITH (pages_per_range='16');"


def clean_btree_index(table="history", year=2011, month=12):
    index = get_index_name(table=table, year=year, month=month, kind="btree")
    yield f"DROP INDEX IF EXISTS {index};"


def clean_old_items(table="history", year=2011, month=12):
    table = get_table_name(table=table, year=year, month=month)
    yield f"DELETE FROM {table} WHERE itemid NOT IN (select itemid from items);"


def create_fit_tables(table="history", year=2011, month=12):
    step = monthdelta.monthdelta(1)
    start_date = datetime(year=year, month=month, day=1,
                          hour=0, minute=0, second=0,
                          tzinfo=pytz.utc)
    stop_date = start_date + step
    month = start_date.month
    start, stop = int(start_date.timestamp()), int(stop_date.timestamp())
    tablename = get_table_name(table=table, year=year, month=month)
    yield f"CREATE TABLE IF NOT EXISTS {tablename} PARTITION OF {table} for values from ({start}) to ({stop});"


def cluster_table(table="history", year=2011, month=12):
    tablename = get_table_name(table=table, year=year, month=month)
    indexname = get_index_name(table=table, year=year, month=month, kind="btree")
    yield from create_btree_index(table=table, year=year, month=month)
    yield f"CLUSTER TABLE {tablename} on {indexname};"
    yield from clean_btree_index(table=table, year=year, month=month)


def do_maintenance(connstr):
    tables = ("history", "history_uint", "history_text", "history_str")

    with psycopg2.connect(connstr) as c:
        c.autocommit = True  # Don't implicitly open a transaction
        for date in gen_current_and_future():
            for table in tables:
                with c.cursor() as curs:
                    for x in create_fit_tables(table=table, year=date.year, month=date.month):
                        curs.execute(x)

                with c.cursor() as curs:
                    for x in create_btree_index(table=table, year=date.year, month=date.month):
                        curs.execute(x)

                with c.cursor() as curs:
                    for x in clean_old_indexes(table=table, year=date.year, month=date.month):
                        curs.execute(x)

        for date in gen_year_past():
            for table in tables:
                with c.cursor() as curs:
                    for x in create_brin_index(table=table, year=date.year, month=date.month):
                        curs.execute(x)

                with c.cursor() as curs:
                    for x in clean_btree_index(table=table, year=date.year, month=date.month):
                        curs.execute(x)

                with c.cursor() as curs:
                    for x in clean_old_indexes(table=table, year=date.year, month=date.month):
                        curs.execute(x)

                with c.cursor() as curs:
                    for x in clean_old_items(table=table, year=date.year, month=date.month):
                        curs.execute(x)

                # TODO: Figure out how/when to run the clustering operation
                if False:
                    with c.cursor() as curs:
                        for x in cluster_table(table=table, year=date.year, month=date.month):
                            curs.execute(x)


def main():
    connstr = connstring()
    do_maintenance(connstr=connstr)


if __name__ == '__main__':
    main()
