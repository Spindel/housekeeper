#!/usr/bin/env python

from __future__ import print_function

import psycopg2
import os


from datetime import (
    datetime,
    date,
)

from .helpers import (
    connstring,
    get_table_name,
    prev_month,
    get_month_before_retention,
)


def get_retention():
    """
    environment variable MODIO_RETENTION in days is used to decide on how many days
    of data retention there should be for all HISTORY data.
    """
    retention = os.environ.get("MODIO_RETENTION")
    retention = int(retention)
    return retention


def remove_old_table(table="history", year=2011, month=12):
    tablename = get_table_name(table=table, year=year, month=month)
    cleanup = f"DROP TABLE IF EXISTS {tablename};"
    yield cleanup


def migrate_old_data(table="history", year=2011, month=12):
    """This function should return data to migrate the partition to an external
    source"""
    tablename = get_table_name(table=table, year=year, month=month)
    del tablename
    action = "SELECT 1;"
    yield action


def work_backwards(day=None):
    if day is None:
        day = datetime.utcnow().date()
    assert isinstance(day, date)

    while day.year >= 2011:
        yield day
        day = prev_month(day)


def main():
    days = get_retention()

    start = get_month_before_retention(retention=days)
    connstr = connstring()
    tables = ("history", "history_uint", "history_text", "history_str")
    with psycopg2.connect(connstr) as c:
        c.autocommit = True  # Don't implicitly open a transaction
        for day in work_backwards(day=start):
            for table in tables:
                with c.cursor() as curs:
                    for x in migrate_old_data(table=table, year=day.year, month=day.month):
                        curs.execute(x)

                    for x in remove_old_table(table=table, year=day.year, month=day.month):
                        curs.execute(x)


if __name__ == '__main__':
    main()
