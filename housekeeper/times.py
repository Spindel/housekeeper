from datetime import (
    datetime,
    timedelta,
    date,
)
from typing import (
    Iterator,
    Optional,
    Callable,
    Tuple,
)

EPOCH = date(1970, 1, 1)
MONTHISH = timedelta(days=31)


def timestamp(day: date) -> int:
    return int((day - EPOCH).total_seconds())


def prev_month(day: date) -> date:
    return (day.replace(day=15) - MONTHISH).replace(day=1)


def next_month(day: date) -> date:
    return (day.replace(day=15) + MONTHISH).replace(day=1)


def get_start_and_stop(*, year: int=2011, month: int=11) -> Tuple[int, int]:
    start = date(year=year, month=month, day=1)
    stop = next_month(start)
    return timestamp(start), timestamp(stop)


def gen_last_month() -> Iterator[date]:
    start_date = datetime.utcnow().date()
    yield prev_month(start_date)


def gen_next_month() -> Iterator[date]:
    start_date = datetime.utcnow().date()
    yield next_month(start_date)


def gen_monthdeltas(*,
                    from_date: Optional[date]=None,
                    step: Callable[[date], date]=next_month) -> Iterator[date]:
    if from_date is None:
        from_date = datetime.utcnow().date()

    day = from_date.replace(day=1)

    yield day
    while True:
        day = step(day)
        yield day


def gen_quarters(start: date) -> Iterator[date]:
    start = start.replace(month=1, day=1)
    yield start
    for x in range(4):
        start = next_month(start)
        start = next_month(start)
        start = next_month(start)
        yield start


def months_for_year_ahead(start: date) -> Iterator[date]:
    months = gen_monthdeltas(from_date=start, step=next_month)
    for n in range(13):
        yield next(months)


def months_for_year_past(start: date) -> Iterator[date]:
    months = gen_monthdeltas(from_date=start, step=prev_month)
    next(months)
    for n in range(13):
        yield next(months)


def deduct_days(start: date, retention: int) -> date:
    assert retention > 0

    offset = retention * 86400
    start_ts = timestamp(start) - offset
    return datetime.utcfromtimestamp(start_ts).date()


def get_month_before_retention(start: Optional[date]=None,
                               retention: int=0) -> date:
    assert retention > 0
    if start is None:
        start = datetime.utcnow().date()

    beginning = deduct_days(start, retention)
    return prev_month(beginning)


def months_between(*, from_date: Optional[date]=None, to_date: date) -> Iterator[date]:
    if from_date is None:
        from_date = date(year=2014, month=1, day=1)
    months = gen_monthdeltas(from_date=from_date)
    month = next(months)
    while month < to_date:
        yield month
        month = next(months)


def months_2014_to_current() -> Iterator[date]:
    start = date(year=2014, month=1, day=1)
    end = datetime.now().date().replace(day=1)
    yield from months_between(from_date=start, to_date=end)
