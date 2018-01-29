#!/usr/bin/env python

from __future__ import print_function

import psycopg2
import time
import sys
import os


def main():
    DBCONFIG = "/etc/zabbix/zabbix.conf.d/database.conf"
    if not os.path.isfile("/etc/zabbix/zabbix.conf.d/database.conf"):
        print("No database config in {}".format(DBCONFIG))
        sys.exit(1)
    """
# DB settings\n
    DBHost=db1.modio.dcl1.synotio.net
    DBName=moodio.se
    DBUser=moodio.se
    DBPassword=sassabrassa booh
    DBPort=5432
    """

    cutoff = int(time.time()) - 2 * 86400
    dhost = "localhost"
    dbname = "zabbix"
    dbuser = "zabbix"
    dbport = "5432"
    dbpass = ""

    with open(DBCONFIG) as f:
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

    with psycopg2.connect(connstr % (dbname, dbuser, dbhost, dbport, dbpass)) as c:
        c.autocommit = True  # Don't implicitly open a transaction
        with c.cursor() as curs:
            curs.execute("DROP INDEX IF EXISTS history_backpeek_temp;")
            curs.execute("CREATE INDEX CONCURRENTLY history_backpeek_temp ON history (itemid, clock) WHERE clock >= %s" % cutoff)
            curs.execute("DROP INDEX IF EXISTS history_backpeek;")
            curs.execute("ALTER INDEX history_backpeek_temp RENAME TO history_backpeek;")
        with c.cursor() as curs:
            curs.execute("DROP INDEX IF EXISTS history_uint_backpeek_temp;")
            curs.execute("CREATE INDEX CONCURRENTLY history_uint_backpeek_temp ON history_uint (itemid, clock) WHERE clock >= %s" % cutoff)
            curs.execute("DROP INDEX IF EXISTS history_uint_backpeek;")
            curs.execute("ALTER INDEX history_uint_backpeek_temp RENAME TO history_uint_backpeek;")
        with c.cursor() as curs:
            curs.execute("DROP INDEX IF EXISTS history_str_backpeek_temp;")
            curs.execute("CREATE INDEX CONCURRENTLY history_str_backpeek_temp ON history_str (itemid, clock) WHERE clock >= %s" % cutoff)
            curs.execute("DROP INDEX IF EXISTS history_str_backpeek;")
            curs.execute("ALTER INDEX history_str_backpeek_temp RENAME TO history_str_backpeek;")
        with c.cursor() as curs:
            curs.execute("DROP INDEX IF EXISTS history_text_backpeek_temp;")
            curs.execute("CREATE INDEX CONCURRENTLY history_text_backpeek_temp ON history_text (itemid, clock) WHERE clock >= %s" % cutoff)
            curs.execute("DROP INDEX IF EXISTS history_text_backpeek;")
            curs.execute("ALTER INDEX history_text_backpeek_temp RENAME TO history_text_backpeek;")
