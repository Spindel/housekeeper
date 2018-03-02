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


def get_table_name(table="history", year=2011, month=12):
    return f"{table}_y{year}m{month:02d}"


def get_index_name(table="history", year=2011, month=12, kind="btree"):
    tablename = get_table_name(table=table, year=year, month=month)
    return f"{tablename}_{kind}_idx"


def get_constraint_name(table="history", year=2011, month=12):
    return f"{table}_y{year}m{month:02d}_check"


def get_start_and_stop(year=2011, month=11):
    step = monthdelta.monthdelta(1)
    start_date = datetime(year=year, month=month, day=1,
                          hour=0, minute=0, second=0,
                          tzinfo=pytz.utc)
    stop_date = start_date + step
    start, stop = int(start_date.timestamp()), int(stop_date.timestamp())
    return start, stop


def gen_last_month():
    step = monthdelta.monthdelta(1)
    start_date = datetime.utcnow() - step

    date = datetime(year=start_date.year, month=start_date.month,
                    day=1, hour=0, minute=0, second=0, tzinfo=pytz.utc)
    yield date


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


def gen_year_past(start=None):
    if start is None:
        start = datetime.utcnow()
    step = monthdelta.monthdelta(13)
    start = start - step
    yield from gen_current_and_future(date=start)
