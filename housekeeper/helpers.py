import sys
import os

from datetime import (
    datetime,
    timedelta,
    date,
)


DBCONFIG = "/etc/zabbix/zabbix.conf.d/database.conf"
EPOCH = date(1970, 1, 1)
MONTHISH = timedelta(days=31)


def timestamp(day):
    assert isinstance(day, date)
    return int((day - EPOCH).total_seconds())


def prev_month(day):
    assert isinstance(day, date)
    return (day.replace(day=15) - MONTHISH).replace(day=1)


def next_month(day):
    assert isinstance(day, date)
    return (day.replace(day=15) + MONTHISH).replace(day=1)


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
    start = date(year=year, month=month, day=1)
    stop = next_month(start)
    return timestamp(start), timestamp(stop)


def gen_last_month():
    start_date = datetime.utcnow().date()
    yield prev_month(start_date)


def gen_next_month():
    start_date = datetime.utcnow().date()
    yield next_month(start_date)


def gen_monthdeltas(*, from_date=None, step=next_month):
    if from_date is None:
        from_date = datetime.utcnow().date()

    day = from_date.replace(day=1)

    yield day
    while True:
        day = step(day)
        yield day


def gen_current_and_future(date=None):
    months = gen_monthdeltas(from_date=date, step=next_month)
    for n in range(13):
        yield next(months)


def gen_quarters(start):
    assert isinstance(start, date)
    start = start.replace(month=1, day=1)
    yield start
    for x in range(4):
        start = next_month(start)
        start = next_month(start)
        start = next_month(start)
        yield start


def gen_year_past(start=None):
    months = gen_monthdeltas(from_date=start, step=prev_month)
    next(months)
    for n in range(13):
        yield next(months)


def deduct_retention(start, retention):
    assert retention > 0
    assert isinstance(start, date)

    offset = retention * 86400
    start_ts = timestamp(start) - offset
    return datetime.utcfromtimestamp(start_ts).date()


def get_month_before_retention(start=None, retention=None):
    assert retention > 0
    if start is None:
        start = datetime.utcnow().date()
    assert isinstance(start, date)

    beginning = deduct_retention(start, retention)
    return prev_month(beginning)
