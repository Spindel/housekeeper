#!/usr/bin/env python3

import psycopg2
import sys
import os

import monthdelta
import pytz

from datetime import datetime

DBCONFIG = "/etc/zabbix/zabbix.conf.d/database.conf"


def connstring(filename=DBCONFIG):
    if not os.path.isfile(filename):
        print("No database config in {}".format(filename))
        sys.exit(1)
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

    connstr = "dbname='%s' user='%s' host='%s' port='%s' password='%s'"
    return connstr % (dbname, dbuser, dbhost, dbport, dbpass)


def gen_current_and_future(frm=None):
    if frm is None:
        frm = datetime.utcnow()
    else:
        start = frm

    start = datetime(
            year=frm.year,
            month=frm.month,
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
    yield from gen_current_and_future(frm=start)


def clean_old_indexes(table="history", year=2011, month=12):
    cleanup = "DROP INDEX IF EXISTS {};"
    tablename = "{table}_y{year}m{month:02d}".format(table=table,
                                                     year=year,
                                                     month=month)
    oldindexes = ["{}_itemid_clock_idx".format(tablename),
                  "{}_itemid_clock_idx1".format(tablename),
                  "{}_itemid_clock_idx2".format(tablename)]
    for oldindex in oldindexes:
        yield cleanup.format(oldindex)


def get_table_name(table="history", year=2011, month=12):
    tablename = "{table}_y{year}m{month:02d}"
    return tablename.format(table=table, year=year, month=month)


def get_index_name(table="history", year=2011, month=12, typ="btree"):
    tablename = get_table_name(table=table, year=year, month=month)
    indexname = "{tablename}_{typ}_idx"
    return indexname.format(typ=typ, tablename=tablename)


def create_btree_index(table="history", year=2011, month=12):
    create = "CREATE INDEX IF NOT EXISTS {index} on {table} using btree (itemid, clock);"
    indexname = get_index_name(table=table, year=year, month=month, typ="btree")
    tablename = get_table_name(table=table, year=year, month=month)
    yield create.format(table=tablename, index=indexname)


def create_brin_index(table="history", year=2011, month=12):
    create = ("CREATE INDEX IF NOT EXISTS {index} on {table} "
              "using brin (itemid, clock) WITH (pages_per_range='16');")
    indexname = get_index_name(table=table, year=year, month=month, typ="brin")
    tablename = get_table_name(table=table, year=year, month=month)
    yield create.format(table=tablename, index=indexname)


def clean_btree_index(table="history", year=2011, month=12):
    cleanup = "DROP INDEX IF EXISTS {index};"
    indexname = get_index_name(table=table, year=year, month=month, typ="btree")
    yield cleanup.format(index=indexname)


def clean_old_items(table="history", year=2011, month=12):
    cleanup = "DELETE FROM {table} WHERE itemid NOT IN (select itemid from items);"
    tablename = get_table_name(table=table, year=year, month=month)
    yield cleanup.format(table=tablename)


def create_fit_tables(table="history", year=2011, month=12):
    step = monthdelta.monthdelta(1)
    start_date = datetime(year=year, month=month, day=1,
                          hour=0, minute=0, second=0,
                          tzinfo=pytz.utc)
    stop_date = start_date + step
    month = start_date.month
    start, stop = int(start_date.timestamp()), int(stop_date.timestamp())
    tablename = get_table_name(table=table, year=year, month=month)
    create = ("CREATE TABLE IF NOT EXISTS {tablename} PARTITION OF {table}"
              " for values from ({start}) to ({stop});")
    yield create.format(table=table, tablename=tablename, start=start, stop=stop)


def cluster_table(table="history", year=2011, month=12):
    cluster = "CLUSTER TABLE {table} on {index};"
    tablename = get_table_name(table=table, year=year, month=month)
    indexname = get_index_name(table=table, year=year, month=month, typ="btree")
    yield from create_btree_index(table=table, year=year, month=month)
    yield cluster.format(table=tablename, index=indexname)
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
