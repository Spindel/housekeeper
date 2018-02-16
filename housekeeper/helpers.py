import sys
import os
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
