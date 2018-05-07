import datetime
import unittest

from . import times


def date(year, month, day):
    return datetime.date(year, month, day)


class TestDateGeneration(unittest.TestCase):
    def test_future_months_at_start_of_month_should_be_correct(self):
        start = date(2018, 3, 1)
        months = [
            date(2018, 3, 1),
            date(2018, 4, 1),
            date(2018, 5, 1),
            date(2018, 6, 1),
            date(2018, 7, 1),
            date(2018, 8, 1),
            date(2018, 9, 1),
            date(2018, 10, 1),
            date(2018, 11, 1),
            date(2018, 12, 1),
            date(2019, 1, 1),
            date(2019, 2, 1),
            date(2019, 3, 1),
        ]
        assert list(times.months_for_year_ahead(start)) == months

    def test_future_months_at_end_of_month_should_be_correct(self):
        start = date(2018, 3, 31)
        months = [
            date(2018, 3, 1),
            date(2018, 4, 1),
            date(2018, 5, 1),
            date(2018, 6, 1),
            date(2018, 7, 1),
            date(2018, 8, 1),
            date(2018, 9, 1),
            date(2018, 10, 1),
            date(2018, 11, 1),
            date(2018, 12, 1),
            date(2019, 1, 1),
            date(2019, 2, 1),
            date(2019, 3, 1),
        ]
        assert list(times.months_for_year_ahead(start)) == months

    def test_past_months_at_start_of_month_should_be_correct(self):
        start = date(2018, 3, 1)
        months = [
            date(2018, 2, 1),
            date(2018, 1, 1),
            date(2017, 12, 1),
            date(2017, 11, 1),
            date(2017, 10, 1),
            date(2017, 9, 1),
            date(2017, 8, 1),
            date(2017, 7, 1),
            date(2017, 6, 1),
            date(2017, 5, 1),
            date(2017, 4, 1),
            date(2017, 3, 1),
            date(2017, 2, 1),
        ]
        assert list(times.months_for_year_past(start)) == months

    def test_past_months_at_end_of_month_should_be_correct(self):
        start = date(2018, 3, 31)
        months = [
            date(2018, 2, 1),
            date(2018, 1, 1),
            date(2017, 12, 1),
            date(2017, 11, 1),
            date(2017, 10, 1),
            date(2017, 9, 1),
            date(2017, 8, 1),
            date(2017, 7, 1),
            date(2017, 6, 1),
            date(2017, 5, 1),
            date(2017, 4, 1),
            date(2017, 3, 1),
            date(2017, 2, 1),
        ]
        assert list(times.months_for_year_past(start)) == months

    def test_get_start_and_stop_matches_utc(self):
        start, stop = times.get_start_and_stop(year=2018, month=2)
        assert start == 1517443200
        assert stop == 1519862400

    def test_get_month_before_retention_gets_prev_month(self):
        start = date(2018, 3, 8)
        day = times.get_month_before_retention(start=start, retention=7)
        assert day == date(2018, 2, 1)

    def test_get_month_before_retention_handles_decrement(self):
        start = date(2018, 3, 6)
        day = times.get_month_before_retention(start=start, retention=7)
        assert day == date(2018, 1, 1)

    def test_gen_quarters(self):
        start = date(2017, 2, 12)
        months = [
            date(2017, 1, 1),
            date(2017, 4, 1),
            date(2017, 7, 1),
            date(2017, 10, 1),
            date(2018, 1, 1),
        ]
        assert list(times.gen_quarters(start)) == months

    def test_timestamp_returns_correct(self):
        start = date(2018, 3, 8)
        result = times.timestamp(start)
        assert isinstance(result, int)
        assert result == 1520467200  # Thu  8 Mar 01:00:00 CET 2018
