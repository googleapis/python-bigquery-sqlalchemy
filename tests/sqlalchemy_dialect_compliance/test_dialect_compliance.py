import pytest
from sqlalchemy import and_
from sqlalchemy.testing.assertions import eq_
from sqlalchemy.testing.suite import *
from sqlalchemy.testing.suite import (
    DateTest as _DateTest,
    DateTimeTest as _DateTimeTest,
    TimeTest as TimeTest,
    DateTimeCoercedToDateTimeTest as _DateTimeCoercedToDateTimeTest,
    DateTimeMicrosecondsTest as _DateTimeMicrosecondsTest,
    TimeMicrosecondsTest as _TimeMicrosecondsTest,
    TextTest as TextTest,
    UnicodeTextTest as UnicodeTextTest,
    UnicodeVarcharTest as UnicodeVarcharTest,
    InsertBehaviorTest as _InsertBehaviorTest,
    ExistsTest as _ExistsTest,
)

# Quotes aren't allowed in BigQuery table names.
del QuotedNameArgumentTest


class NoPrimaryKeySupport(_DateTest):
    """
    Bigquery doesn't support Primary keys

    and has no automatic way to provide values for them.
    """

    @pytest.mark.skip()
    def test_null(cls):
        pass

    test_null_bound_comparison = test_round_trip = test_null


class DateTest(NoPrimaryKeySupport, _DateTest):
    pass


class DateTimeTest(NoPrimaryKeySupport, _DateTimeTest):
    pass


class TimeTest(NoPrimaryKeySupport, _DateTimeTest):
    pass


class DateTimeCoercedToDateTimeTest(NoPrimaryKeySupport, _DateTimeCoercedToDateTimeTest):
    pass


class DateTimeMicrosecondsTest(NoPrimaryKeySupport, _DateTimeMicrosecondsTest):
    pass


class TimeMicrosecondsTest(NoPrimaryKeySupport, _TimeMicrosecondsTest):
    pass


class TextTest(NoPrimaryKeySupport, _DateTimeTest):
    pass


class UnicodeTextTest(NoPrimaryKeySupport, _DateTimeTest):
    pass


class UnicodeVarcharTest(NoPrimaryKeySupport, _DateTimeTest):
    pass


class InsertBehaviorTest(_InsertBehaviorTest):
    """
    Bigquery doesn't support Primary keys

    and has no automatic way to provide values for them.
    """

    @pytest.mark.skip()
    def test_autoclose_on_insert(cls):
        pass

    test_insert_from_select_autoinc = test_autoclose_on_insert


class ExistsTest(_ExistsTest):
    """
    Override

    Becaise Bigquery requires FROM when there's a WHERE and
    the base tests didn't do provide a FROM.
    """

    def test_select_exists(self, connection):
        stuff = self.tables.stuff
        eq_(
            connection.execute(
                select([stuff.c.id]).where(
                    and_(
                        stuff.c.id == 1,
                        exists().where(stuff.c.data == "some data"),
                    )
                )
            ).fetchall(),
            [(1,)],
        )

    def test_select_exists_false(self, connection):
        stuff = self.tables.stuff
        eq_(
            connection.execute(
                select([stuff.c.id]).where(
                    exists().where(stuff.c.data == "no data")
                )
            ).fetchall(),
            [],
        )
