#!/usr/bin/env python

from __future__ import print_function

import psycopg2
import os

import pytz
import monthdelta

from datetime import datetime
from dateutil.relativedelta import relativedelta

from .helpers import (
    connstring,
    get_table_name,
)


def get_retention():
    """
    environment variable MODIO_RETENTION in days is used to decide on how many days
    of data retention there should be for all HISTORY data.
    """
    retention = os.environ.get("MODIO_RETENTION")
    retention = int(retention)
    return retention


def get_month_before_retention(start=None, retention=None):
    if start is None:
        start = datetime.utcnow()
    if retention is None:
        raise ValueError

    month = relativedelta(months=1)
    offset = relativedelta(days=retention)

    date = start - offset
    date = datetime(
        year=date.year,
        month=date.month,
        day=1,
        hour=0,
        minute=0,
        second=0,
        tzinfo=pytz.utc,
    )
    return date - month


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


def work_backwards(timepoint=None):
    step = monthdelta.monthdelta(1)
    if timepoint is None:
        timepoint = datetime.utcnow()

    date = datetime(
            year=timepoint.year,
            month=timepoint.month,
            day=1,
            hour=0,
            minute=0,
            second=0,
            tzinfo=pytz.utc)
    while date.year >= 2011:
        yield date
        date = date - step


def main():
    days = get_retention()

    start = get_month_before_retention(retention=days)
    connstr = connstring()
    tables = ("history", "history_uint", "history_text", "history_str")
    with psycopg2.connect(connstr) as c:
        c.autocommit = True  # Don't implicitly open a transaction
        for date in work_backwards(timepoint=start):
            for table in tables:
                with c.cursor() as curs:
                    for x in migrate_old_data(table=table, year=date.year, month=date.month):
                        curs.execute(x)

                    for x in remove_old_table(table=table, year=date.year, month=date.month):
                        curs.execute(x)


if __name__ == '__main__':
    main()
