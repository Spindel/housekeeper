
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


def move_data(year=2010, to_table="history_part", from_table="history", max_itemid=100000):
    insertline = """insert into {part_table}  select * from {from_table}
                    where clock >= {start} and clock < {stop} and itemid < {max_itemid}
                    order by itemid;"""
    deleteline = """delete from {from_table} where
                    clock >= {start} and clock < {stop} and itemid <{max_itemid};"""

    createline = """create table {part_table} PARTITION OF {to_table}
                     for values from ({start}) to ({stop});"""

    create_index = """create index on {part_table} using brin (itemid, clock)
                      with (pages_per_range=16);"""

    series = gen_series(year=year)
    for x, y in gen_pairs(series):
        month = x.month
        start, stop = int(x.timestamp()), int(y.timestamp())
        part_table = "{table}_y{year}m{month:02d}".format(table=to_table, year=year, month=month)
        yield createline.format(to_table=to_table, part_table=part_table, start=start, stop=stop)

        for x in range(0, max_itemid, 10000):
                yield "BEGIN TRANSACTION ISOLATION LEVEL SERIALIZABLE;"
                yield insertline.format(part_table=part_table, from_table=from_table,
                                        start=start, stop=stop, max_itemid=x)
                yield deleteline.format(from_table=from_table, start=start, stop=stop, max_itemid=x)
                yield "END TRANSACTION;"
                yield create_index.format(part_table=part_table)


def rename_table(old="history", new="history_old"):
    return "ALTER TABLE {old} rename to {new};".format(old=old, new=new)


if __name__ == "__main__":
    tables = ("history", "history_str", "history_text", "history_uint")
    for table in tables:
        from_table = table
        old_table = "{}_old".format(table)
        to_table = "{}_part".format(table)
        print(gen_base_partition(from_table=from_table, to_table=to_table))
        for year in (2018, 2019, 2020):
            for x in move_data(year=year, from_table=from_table, to_table=to_table):
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
        to_table = "{}_part".format(table)
        for year in (2014, 2015, 2016, 2017):
            for x in move_data(year=year, from_table=old_table, to_table=to_table):
                print(x)
