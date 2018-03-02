import datetime
import unittest

import pytz

from . import helpers


def date(year, month, day):
    return datetime.datetime(year, month, day, tzinfo=pytz.utc)


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
        assert list(helpers.gen_current_and_future(start)) == months

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
        assert list(helpers.gen_current_and_future(start)) == months

    def test_past_months_at_start_of_month_should_be_correct(self):
        start = date(2018, 3, 1)
        months = [
            date(2017, 2, 1),
            date(2017, 3, 1),
            date(2017, 4, 1),
            date(2017, 5, 1),
            date(2017, 6, 1),
            date(2017, 7, 1),
            date(2017, 8, 1),
            date(2017, 9, 1),
            date(2017, 10, 1),
            date(2017, 11, 1),
            date(2017, 12, 1),
            date(2018, 1, 1),
            date(2018, 2, 1),
        ]
        assert list(helpers.gen_year_past(start)) == months

    def test_past_months_at_end_of_month_should_be_correct(self):
        start = date(2018, 3, 31)
        months = [
            date(2017, 2, 1),
            date(2017, 3, 1),
            date(2017, 4, 1),
            date(2017, 5, 1),
            date(2017, 6, 1),
            date(2017, 7, 1),
            date(2017, 8, 1),
            date(2017, 9, 1),
            date(2017, 10, 1),
            date(2017, 11, 1),
            date(2017, 12, 1),
            date(2018, 1, 1),
            date(2018, 2, 1),
        ]
        assert list(helpers.gen_year_past(start)) == months
