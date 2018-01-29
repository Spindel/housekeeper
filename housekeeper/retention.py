#!/usr/bin/env python

from __future__ import print_function

import psycopg2
import time
import sys
import os


def main():
    """
    environment variable MODIO_RETENTION in days is used to decide on how many days
    of data retention there should be for all HISTORY data.
    """
    retention = os.environ.get("MODIO_RETENTION", "0")
    retention = int(retention)

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
            curs.execute("DELETE FROM history WHERE itemid NOT IN "
                         "(SELECT itemid from items);")
        with c.cursor() as curs:
            curs.execute("DELETE FROM history_uint WHERE itemid NOT IN "
                         "(SELECT itemid from items);")
        with c.cursor() as curs:
            curs.execute("DELETE FROM history_str WHERE itemid NOT IN "
                         "(SELECT itemid from items);")
        with c.cursor() as curs:
            curs.execute("DELETE FROM history_text WHERE itemid NOT IN "
                         "(SELECT itemid from items);")
        with c.cursor() as curs:
            curs.execute("DELETE FROM history_log WHERE itemid NOT IN "
                         "(SELECT itemid from items);")
        with c.cursor() as curs:
            curs.execute("DELETE FROM trends WHERE itemid NOT IN "
                         "(SELECT itemid from items);")
        with c.cursor() as curs:
            curs.execute("DELETE FROM trends_uint WHERE itemid NOT IN "
                         "(SELECT itemid from items);")

        if retention > 7:
            now = int(time.time())
            cutoff = now - (retention * 86400)
            with c.cursor() as curs:
                curs.execute("DELETE FROM history WHERE clock < %s;" % cutoff)
            with c.cursor() as curs:
                curs.execute("DELETE FROM history_uint WHERE clock < %s;" % cutoff)
            with c.cursor() as curs:
                curs.execute("DELETE FROM history_str WHERE clock < %s;" % cutoff)
            with c.cursor() as curs:
                curs.execute("DELETE FROM history_text WHERE clock < %s;" % cutoff)
            with c.cursor() as curs:
                curs.execute("DELETE FROM history_log WHERE clock < %s;" % cutoff)
