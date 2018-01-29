
import monthdelta
import pytz

from datetime import datetime


def gen_series(year=2015):
    start = datetime(year=year, month=1, day=1,
                     hour=0, minute=0, second=0,
                     tzinfo=pytz.utc)
    step = monthdelta.monthdelta(1)
    yield start
    for x in range(12):
        start = start + step
        yield start


def gen_quarters(year=2015):
    start = datetime(year=year, month=1, day=1,
                     hour=0, minute=0, second=0,
                     tzinfo=pytz.utc)
    step = monthdelta.monthdelta(3)
    yield start
    for x in range(4):
        start = start + step
        yield start


def gen_quarterly_partitions(year=2010, from_table="history",
                             partition_basename="history"):
    pass

def gen_pairs(series):
    first = list(series)
    second = iter(first)
    next(second)
    for x, y in zip(first, second):
        yield x, y


def gen_partitions(year=2010, table="history_part"):
    series = gen_series(year=year)
    for x, y in gen_pairs(series):
        month = x.month
        start, stop = int(x.timestamp()), int(y.timestamp())
        line = """create table {table}_y{year}m{month:02d} PARTITION OF {table}
                  for values from ({start}) to ({stop});"""
        yield line.format(table=table, year=year, month=month,
                          start=start, stop=stop)


def gen_base_partition(from_table="history", to_table="history_part"):
    msg = """CREATE TABLE {to_table} (like {from_table}) partition by range(clock);"""
    return msg.format(from_table=from_table, to_table=to_table)


def move_data(year=2010, partition_basename="history",
                         to_table="history_part", from_table="history", max_itemid=100000):

    createline = """create table {part_table} PARTITION OF {to_table}
                     for values from ({start}) to ({stop});"""

    indexold = """create index on {from_table} using btree(itemid,clock) where
                  clock >= {start} and clock <{stop}"""

    insertline = """insert into {part_table}  select * from {from_table}
                    where clock >= {start} and clock < {stop} and
                    itemid >= {min_itemid} and itemid <{max_itemid}
                    order by itemid;"""

    deleteline = """delete from {from_table} where
                    clock >= {start} and clock < {stop} and
                    itemid >= {min_itemid} and itemid <{max_itemid};"""


    create_index = """create index on {part_table} using brin (itemid, clock)
                      with (pages_per_range=16);"""

    vacuum = """vauum {table};"""

    series = gen_series(year=year)
    for x, y in gen_pairs(series):
        month = x.month
        start, stop = int(x.timestamp()), int(y.timestamp())
        part_table = "{table}_y{year}m{month:02d}".format(table=partition_basename, year=year, month=month)
        yield createline.format(to_table=to_table, part_table=part_table, start=start, stop=stop)
        yield indexold.format(from_table=from_table, start=start, stop=stop)
        prev = 0
        # 1000 is a magical constant that makes postgres prefer an index scan
        # over sequential scan.
        for x in range(0, max_itemid, 1000):
            yield "BEGIN TRANSACTION ISOLATION LEVEL SERIALIZABLE;"
            yield insertline.format(part_table=part_table, from_table=from_table,
                                    start=start, stop=stop,
                                    min_itemid=prev, max_itemid=x)
            yield deleteline.format(from_table=from_table, start=start, stop=stop,
                                    min_itemid=prev, max_itemid=x)
            yield "END TRANSACTION;"
            prev = x
        yield create_index.format(part_table=part_table)
        yield vacuum.format(table=old_table)


def rename_table(old="history", new="history_old"):
    return "ALTER TABLE {old} rename to {new};".format(old=old, new=new)


def main():
    print("set role postgres;")
    itemcount = 50000
    tables = ("history", "history_str", "history_text", "history_uint")
    for table in tables:
        from_table = table
        old_table = "{}_old".format(table)
        to_table = "{}_part".format(table)
        print(gen_base_partition(from_table=from_table, to_table=to_table))
        for year in (2018, ):
            for x in move_data(year=year, partition_basename=table,
                               from_table=from_table, to_table=to_table,
                               max_itemid=itemcount):
                print(x)
        print("BEGIN TRANSACTION ISOLATION LEVEL SERIALIZABLE;")
        print(rename_table(old=from_table, new=old_table))
        print(rename_table(old=to_table, new=from_table))
        print("END TRANSACTION;")
    print("-- cut here")
    print("-- --8<-- --8<-- --8<-- CUT HERE--8<-- --8<-- --8<-- --8<-- ")
    print("-- After this point you can run your load as normal --")
    for table in tables:
        old_table = "{}_old".format(table)
        to_table = table
        for year in (2014, 2015, 2016, 2017):
            for x in move_data(year=year, partition_basename=table,
                               from_table=old_table, to_table=to_table,
                               max_itemid=itemcount):
                print(x)

if __name__ == "__main__":
    main()
