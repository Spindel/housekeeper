#!/usr/bin/env python3
import sys
import psycopg2

import datetime

from .helpers import (
    connstring,
    execute,
    get_constraint_name,
    get_index_name,
    get_table_name,
    table_exists,
)

from .times import (
    months_for_year_ahead,
    months_for_year_past,
    gen_last_month,
    get_start_and_stop,
    months_2014_to_current,
)

FAST_WINDOW = 14

def clean_old_indexes(table="history", year=2011, month=12):
    tablename = get_table_name(table=table, year=year, month=month)
    oldindexes = [
        f"{tablename}_itemid_clock_idx",
        f"{tablename}_itemid_clock_idx1",
        f"{tablename}_itemid_clock_idx2",
    ]
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
    yield (
        f"CREATE INDEX CONCURRENTLY IF NOT EXISTS {index} on {table} "
        f"USING brin (itemid, clock) WITH (pages_per_range='16');"
    )


def clean_btree_index(table="history", year=2011, month=12):
    index = get_index_name(table=table, year=year, month=month, kind="btree")
    yield f"DROP INDEX IF EXISTS {index};"


def clean_old_items(table="history", year=2011, month=12):
    table = get_table_name(table=table, year=year, month=month)
    yield f"DELETE FROM {table} WHERE itemid NOT IN (select itemid FROM items);"


def vacuum_table(table="history", year=2011, month=12):
    """Vacuums the table. Because you asked for it"""
    table = get_table_name(table=table, year=year, month=month)
    yield f"VACUUM ANALYZE {table};"


def clean_duplicate_items(table="history", year=2011, month=12, count_seconds=33613):
    """In small batches, delete duplicated rows from history tables.
    The time logic is a bit hairy, and the DELETE SQL is worse than that.

    Group by all itemid, clock, value, ns (in a sub-select) to get all
    duplicate rows, then use ctid to ensure uniqueness.

    We don't parse the entire month at once, but in minor batches to make life
    better for the database and cut down on amount of temp/sort space needed.
    """

    partition = get_table_name(table=table, year=year, month=month)
    start_time, end_time = get_start_and_stop(year=year, month=month)
    start = start_time
    stop = start + count_seconds
    count = 0

    while stop <= end_time:
        # This operation may cause a LOT of churn and is helped by a
        # functional vacuum.

        # Because we batch on smaller groups, to consume less memory, it's
        # important that we sometimes have working statistics, otherwise a
        # delete query towards the end of a month will have enough churn in the
        # blocks to cause DELETE queries to block for several days.
        # 11 is a fun palindrome and prime.
        if count % 11 == 0:
            yield from vacuum_table(table=table, year=year, month=month)

        yield f"""DELETE
FROM {partition} T1
USING (
      SELECT MIN(ctid) as ctid,
             {partition}.*
      FROM   {partition}
      GROUP BY (
             {partition}.itemid,
             {partition}.clock,
             {partition}.value,
             {partition}.ns
      )
      HAVING COUNT(*) > 1 ) T2
WHERE T1.ctid <> T2.ctid
AND  T1.clock BETWEEN {start} AND {stop}
AND  T2.clock BETWEEN {start} AND {stop}
AND  T1.itemid = T2.itemid
AND  T1.clock = T2.clock
AND  T1.value = T2.value
AND  T1.ns = T2.ns;"""

        start = stop
        stop = start + count_seconds
        count += 1

    # Always vacuum before we leave, as we may have caused churn on the table
    yield from vacuum_table(table=table, year=year, month=month)


def clean_expired_items(table="history", year=2012, month=12, retention=FAST_WINDOW):
    """Generates a DELETE statement on the table to clean out "old" data.

    Old is defined as the zabbix way, "items.history" is in days, and compared to
    our `retention` input data days"""
    retention = int(retention)
    if retention < 14:
        raise ValueError("We do not touch the 14 days of fast data.")
    tablename = get_table_name(table=table, year=year, month=month)
    # extract('epoch' from timestamp)  Gets the unix timestamp
    # interval '14 days'  # is a range of 14-days
    # item.history is in days
    yield f"""DELETE
FROM {tablename}
WHERE {tablename}.itemid IN (
    SELECT itemid
    FROM items
    WHERE items.history < {retention}
)
AND {tablename}.clock <  extract('epoch' from current_timestamp - interval '{retention} days');
"""


def create_item_statistics():
    yield "CREATE STATISTICS IF NOT EXISTS s_items ON itemid, name, key_, hostid FROM items;"


def create_statistics(table="history"):
    yield f"CREATE STATISTICS IF NOT EXISTS s_{table} ON itemid, clock FROM {table};"


def create_table_partition(table="history", year=2011, month=12):
    start, stop = get_start_and_stop(year=year, month=month)
    tablename = get_table_name(table=table, year=year, month=month)
    yield f"CREATE TABLE IF NOT EXISTS {tablename} PARTITION OF {table} FOR values FROM ({start}) TO ({stop});"


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
    constraint = (
        f"ALTER TABLE {tablename} ADD CONSTRAINT {constraint_name} "
        f"CHECK (clock >= {start} AND clock < {stop});"
    )
    yield from drop_check_constraint(table=table, year=year, month=month)
    yield constraint


def attach_partition(table="history", year=2011, month=12):
    start, stop = get_start_and_stop(year=year, month=month)
    partition_name = get_table_name(table=table, year=year, month=month)
    attach = f"ALTER TABLE {table} ATTACH PARTITION {partition_name} FOR VALUES FROM ({start}) TO ({stop});"
    yield attach


def do_cluster_operation(table="history", year=2011, month=12):
    """Clusters a table by creating a btree_index on (itemid, clock) and then
    clustering it (locking it exclusively), finally removing the unnecessary
    index."""
    tablename = get_table_name(table=table, year=year, month=month)
    indexname = get_index_name(table=table, year=year, month=month, kind="btree")
    start, stop = get_start_and_stop(year=year, month=month)

    yield from ensure_btree_index(table=table, year=year, month=month)
    yield f"CLUSTER {tablename} USING {indexname};"
    yield from add_check_constraint(table=table, year=year, month=month)
    yield from clean_btree_index(table=table, year=year, month=month)


def cluster_table(table="history", year=2011, month=12):
    tablename = get_table_name(table=table, year=year, month=month)
    start, stop = get_start_and_stop(year=year, month=month)
    temp_table = f"{tablename}_temp"

    yield "BEGIN TRANSACTION;"
    yield from detach_partition(table=table, year=year, month=month)
    yield f"CREATE TABLE IF NOT EXISTS {temp_table} PARTITION OF {table} for values from ({start}) to ({stop});"
    yield "COMMIT;"

    yield from do_cluster_operation(table=table, year=year, month=month)

    yield "BEGIN TRANSACTION;"
    yield f"ALTER TABLE {table} DETACH PARTITION {temp_table};"
    yield from attach_partition(table=table, year=year, month=month)
    yield "COMMIT;"

    yield "BEGIN TRANSACTION;"
    yield f"INSERT INTO {tablename} SELECT * from {temp_table} order by itemid,clock;"
    yield f"DROP TABLE {temp_table};"
    yield "COMMIT;"

    yield from drop_check_constraint(table=table, year=year, month=month)


def migrate_config_items():
    yield "BEGIN TRANSACTION ISOLATION LEVEL SERIALIZABLE;"
    yield (
        "INSERT INTO history_text (id, ns, itemid, clock, value) "
        "  SELECT 0, 0, itemid, clock, value FROM history_str WHERE itemid IN "
        " (SELECT itemid FROM items WHERE name LIKE 'mytemp.internal.conf%' AND value_type=1);"
    )

    yield "UPDATE items SET value_type=4 WHERE name LIKE 'mytemp.internal.conf%';"
    yield "DELETE FROM items WHERE name LIKE 'mytemp.internal.change%';"
    yield (
        "DELETE FROM history_str WHERE itemid IN "
        "  ( SELECT itemid FROM items "
        "     WHERE name LIKE 'mytemp.internal.conf%' AND value_type=4);"
    )
    yield "COMMIT;"


def should_maintain(conn, table="history", year=2112, month=12):
    tbname = get_table_name(table=table, year=year, month=month)
    return table_exists(conn, tbname)


def sql_prelude():
    yield "SET WORK_MEM='1GB';"


def do_maintenance(connstr, cluster=False):
    tables = ("history", "history_uint", "history_text", "history_str")

    with psycopg2.connect(connstr) as c:
        c.autocommit = True  # Don't implicitly open a transaction
        with c.cursor() as curs:
            for statement in sql_prelude():
                execute(curs, statement)

        # Create statistics ( let the auto-analyze function analyze later)
        with c.cursor() as curs:
            for x in create_item_statistics():
                execute(curs, x)
            for table in tables:
                for x in create_statistics(table=table):
                    execute(curs, x)

        for date in months_for_year_ahead():
            for table in tables:
                for x in create_table_partition(
                    table=table, year=date.year, month=date.month
                ):
                    with c.cursor() as curs:
                        execute(curs, x)

                for x in ensure_btree_index(
                    table=table, year=date.year, month=date.month
                ):
                    with c.cursor() as curs:
                        execute(curs, x)

                for x in clean_old_indexes(
                    table=table, year=date.year, month=date.month
                ):
                    with c.cursor() as curs:
                        execute(curs, x)

                for x in ensure_brin_index(
                    table=table, year=date.year, month=date.month
                ):
                    with c.cursor() as curs:
                        execute(curs, x)

        for n, date in enumerate(months_for_year_past()):
            previous_month = n == 0
            for table in tables:
                for x in clean_old_indexes(
                    table=table, year=date.year, month=date.month
                ):
                    with c.cursor() as curs:
                        execute(curs, x)

                if should_maintain(c, table=table, year=date.year, month=date.month):
                    for x in ensure_brin_index(
                        table=table, year=date.year, month=date.month
                    ):
                        with c.cursor() as curs:
                            execute(curs, x)

                if not previous_month:
                    for x in clean_btree_index(
                        table=table, year=date.year, month=date.month
                    ):
                        with c.cursor() as curs:
                            execute(curs, x)

        if cluster:
            for date in gen_last_month():
                for table in tables:
                    # Clean out expired items before we remove duplicates
                    for x in clean_expired_items(table=table, year=date.year, month=date.month, retention=FAST_WINDOW):
                        with c.cursor() as curs:
                            execute(curs, x)
                    # Remove duplicated rows from tables before we cluster them
                    for x in clean_duplicate_items(
                        table=table, year=date.year, month=date.month
                    ):
                        with c.cursor() as curs:
                            execute(curs, x)

                    for x in cluster_table(
                        table=table, year=date.year, month=date.month
                    ):
                        with c.cursor() as curs:
                            execute(curs, x)


def oneshot_maintenance_operation(table="history", year=2018, month=12):
    yield from ensure_brin_index(table=table, year=year, month=month)
    yield from clean_old_indexes(table=table, year=year, month=month)
    yield from clean_old_items(table=table, year=year, month=month)
    yield from clean_expired_items(table=table, year=year, month=month)
    yield from clean_duplicate_items(table=table, year=year, month=month)
    yield from cluster_table(table=table, year=year, month=month)


def maintain_last_year():
    tables = ("history", "history_uint", "history_text", "history_str")
    for date in months_for_year_past():
        for table in tables:
            yield from oneshot_maintenance_operation(table=table, year=date.year, month=date.month)


def oneshot_maintenance():
    tables = ("history", "history_uint", "history_text", "history_str")
    for date in months_2014_to_current():
        for table in tables:
            yield from oneshot_maintenance_operation(table=table, year=date.year, month=date.month)


def do_oneshot_maintenance(connstr):
    tables = ("history", "history_uint", "history_text", "history_str")

    with psycopg2.connect(connstr) as c:
        c.autocommit = True  # Don't implicitly open a transaction
        with c.cursor() as curs:
            for statement in sql_prelude():
                execute(curs, statement)

        # Move config items out
        for x in migrate_config_items():
            with c.cursor() as curs:
                execute(curs, x)

        # Create statistics (let the auto-analyze function analyze later)
        with c.cursor() as curs:
            for x in create_item_statistics():
                execute(curs, x)
            for table in tables:
                for x in create_statistics(table=table):
                    execute(curs, x)

        # And for all tables, do complete maintenance
        for date in months_2014_to_current():
            for table in tables:
                if should_maintain(c, table=table, year=date.year, month=date.month):
                    for x in oneshot_maintenance_operation(table=table, year=date.year, month=date.month):
                        with c.cursor() as curs:
                            execute(curs, x)


def main():
    connstr = connstring()
    command = "help"
    if len(sys.argv) == 1:
        command = "cron"
    elif len(sys.argv) > 1:
        command = sys.argv[-1]

    if command not in ("cron", "cluster", "oneshot"):
        print(f"Usage: {sys.argv[0]} {{ COMMAND }}")
        print("where COMMAND := { cluster | oneshot | cluster_all }")
        print("")
        print("oneshot: Sets up indexes, cleans out items, and clusters all tables from 2014 and onwards. ")
        print("         Extremely heavy operation. ")
        print("cluster: Clusters last month, run in case you missed the cron job the 14th.")
        print("cron: Ensures indexes exist, table partitions exists for the")
        print("      future, and will cluster last month if the date is the 14th")
        print("-")
        print("No arguments: run in cron mode")
        sys.exit(1)

    if command == "cron":
        should_cluster = datetime.datetime.utcnow().day == FAST_WINDOW
        do_maintenance(connstr=connstr, cluster=should_cluster)
    elif command == "cluster":
        do_maintenance(connstr=connstr, cluster=True)
    elif command == "oneshot":
        do_oneshot_maintenance(connstr=connstr)


if __name__ == "__main__":
    assert FAST_WINDOW == 14, "Fast window should be 14 days."
    main()
