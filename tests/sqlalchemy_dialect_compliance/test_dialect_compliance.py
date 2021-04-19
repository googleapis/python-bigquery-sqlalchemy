# Copyright (c) 2021 The PyBigQuery Authors
#
# Permission is hereby granted, free of charge, to any person obtaining a copy of
# this software and associated documentation files (the "Software"), to deal in
# the Software without restriction, including without limitation the rights to
# use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
# the Software, and to permit persons to whom the Software is furnished to do so,
# subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
# FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
# COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER
# IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
# CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

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
    NumericTest as _NumericTest,
    LimitOffsetTest as _LimitOffsetTest,
    RowFetchTest as _RowFetchTest,
)

# Quotes aren't allowed in BigQuery table names.
del QuotedNameArgumentTest


class BQCantGuessTypeForComplexQueries(_DateTest):
    # Like:

    # SELECT `date_table`.`id` AS `date_table_id`
    # FROM `date_table`
    # WHERE CASE WHEN (@`foo` IS NOT NULL)
    #       THEN @`foo` ELSE `date_table`.`date_data` END = `date_table`.`date_data`

    # bind_expression is the hook to fix this n the BQ client side.

    @pytest.mark.skip()
    def test_null_bound_comparison(cls):
        pass


class DateTest(BQCantGuessTypeForComplexQueries, _DateTest):
    pass


class DateTimeTest(BQCantGuessTypeForComplexQueries, _DateTimeTest):
    pass


class TimeTest(BQCantGuessTypeForComplexQueries, TimeTest):
    pass


class DateTimeCoercedToDateTimeTest(BQCantGuessTypeForComplexQueries, _DateTimeCoercedToDateTimeTest):
    pass


class DateTimeMicrosecondsTest(BQCantGuessTypeForComplexQueries, _DateTimeMicrosecondsTest):
    pass


class TimeMicrosecondsTest(BQCantGuessTypeForComplexQueries, _TimeMicrosecondsTest):
    pass


class InsertBehaviorTest(_InsertBehaviorTest):

    @pytest.mark.skip()
    def test_insert_from_select_autoinc(cls):
        """BQ has no autoinc and client-side defaults can't work for select."""


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


class NumericTest(_NumericTest):

    @pytest.mark.skip()
    def saving_values_of_slightly_wrong_type(cls):
        """
        These test want to save a float into a numeric column.

        This should work, but the BigQuery db api interfaces sets
        parameter types by inspecting values and sets the wrong type.

        It's weird that the server can't handle this. :(

        We could:

        - Do a dry-run first to get the types.

        - Extend the BigQuery db api to accept values with type
          markers, because SQLAlchemy knows what the types are and
          could pass them down the call chain.

          (An arguably more elegent variation on this would be to
          build this into the substitution syntax. Something like:
          %(foo:Date)s, but that would be harder to plumb.)
        """

    test_numeric_as_decimal = saving_values_of_slightly_wrong_type
    test_numeric_as_float = saving_values_of_slightly_wrong_type


class LimitOffsetTest(_LimitOffsetTest):

    @pytest.mark.skip()
    def test_simple_offset(self):
        """BigQuery doesn't allow an offset without a limit."""

    test_bound_offset = test_simple_offset


# This test requires features (indexes, primary keys, etc., that BigQuery doesn't have.
del LongNameBlowoutTest

class RowFetchTest(_RowFetchTest):
    # We have to rewrite these tests, because of:
    # https://github.com/googleapis/python-bigquery-sqlalchemy/issues/78

    def test_row_with_dupe_names(self):
        result = config.db.execute(
            select(
                [
                    self.tables.plain_pk.c.data.label("data"),
                    self.tables.plain_pk.c.data.label("data"),
                ]
            ).order_by(self.tables.plain_pk.c.id)
        )
        row = result.first()
        eq_(result.keys(), ["data", "data"])
        eq_(row, ("d1", "d1"))

    def test_via_string(self):
        row = config.db.execute(
            self.tables.plain_pk.select().order_by(self.tables.plain_pk.c.id)
        ).first()

        eq_(row["plain_pk_id"], 1)
        eq_(row["plain_pk_data"], "d1")
