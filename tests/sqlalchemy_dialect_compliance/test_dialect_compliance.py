# Copyright (c) 2021 The sqlalchemy-bigquery Authors
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

import datetime
import decimal
import mock
import packaging.version
import pytest
import pytz
import sqlalchemy
from sqlalchemy import and_

import sqlalchemy.testing.suite.test_types
import sqlalchemy.sql.sqltypes
from sqlalchemy.testing import util, config
from sqlalchemy.testing import is_false
from sqlalchemy.testing import is_true
from sqlalchemy.testing import is_
from sqlalchemy.testing.assertions import eq_
from sqlalchemy.testing.suite import config, select, exists
from sqlalchemy.testing.suite import *  # noqa
from sqlalchemy.testing.suite import (
    ComponentReflectionTest as _ComponentReflectionTest,
    CTETest as _CTETest,
    ExistsTest as _ExistsTest,
    InsertBehaviorTest as _InsertBehaviorTest,
    LongNameBlowoutTest,
    QuotedNameArgumentTest,
    SimpleUpdateDeleteTest as _SimpleUpdateDeleteTest,
    TimestampMicrosecondsTest as _TimestampMicrosecondsTest,
)

from sqlalchemy.testing.suite.test_types import (
    ArrayTest,
)

from sqlalchemy.testing.suite.test_reflection import (
    BizarroCharacterFKResolutionTest,
    ComponentReflectionTest,
    OneConnectionTablesTest,
    HasTableTest,
)

if packaging.version.parse(sqlalchemy.__version__) >= packaging.version.parse("2.0"):
    import uuid
    from sqlalchemy.sql import type_coerce
    from sqlalchemy import Uuid
    from sqlalchemy.testing.suite import (
        TrueDivTest as _TrueDivTest,
        IntegerTest as _IntegerTest,
        NumericTest as _NumericTest,
        DifficultParametersTest as _DifficultParametersTest,
        FetchLimitOffsetTest as _FetchLimitOffsetTest,
        PostCompileParamsTest,
        StringTest as _StringTest,
        UuidTest as _UuidTest,
    )

    class TimestampMicrosecondsTest(_TimestampMicrosecondsTest):
        data = datetime.datetime(2012, 10, 15, 12, 57, 18, 396, tzinfo=pytz.UTC)

        def test_select_direct(self, connection):
            # This func added because this test was failing when passed the
            # UTC timezone.

            def literal(value, type_=None):
                assert value == self.data

                if type_ is not None:
                    assert type_ is self.datatype

                return sqlalchemy.sql.elements.literal(value, self.datatype)

            with mock.patch("sqlalchemy.testing.suite.test_types.literal", literal):
                super(TimestampMicrosecondsTest, self).test_select_direct(connection)

    def test_round_trip_executemany(self, connection):
        unicode_table = self.tables.unicode_table
        connection.execute(
            unicode_table.insert(),
            [{"id": i, "unicode_data": self.data} for i in range(3)],
        )

        rows = connection.execute(select(unicode_table.c.unicode_data)).fetchall()
        eq_(rows, [(self.data,) for i in range(3)])
        for row in rows:
            # 2.0 had no support for util.text_type
            assert isinstance(row[0], str)

    sqlalchemy.testing.suite.test_types._UnicodeFixture.test_round_trip_executemany = (
        test_round_trip_executemany
    )

    class TrueDivTest(_TrueDivTest):
        @pytest.mark.skip("BQ rounds based on datatype")
        def test_floordiv_integer(self):
            pass

        @pytest.mark.skip("BQ rounds based on datatype")
        def test_floordiv_integer_bound(self):
            pass

    class SimpleUpdateDeleteTest(_SimpleUpdateDeleteTest):
        """The base tests fail if operations return rows for some reason."""

        def test_update(self):
            t = self.tables.plain_pk
            connection = config.db.connect()
            # In SQLAlchemy 2.0, the datatype changed to dict in the following function.
            r = connection.execute(t.update().where(t.c.id == 2), dict(data="d2_new"))
            assert not r.is_insert

            eq_(
                connection.execute(t.select().order_by(t.c.id)).fetchall(),
                [(1, "d1"), (2, "d2_new"), (3, "d3")],
            )

        def test_delete(self):
            t = self.tables.plain_pk
            connection = config.db.connect()
            r = connection.execute(t.delete().where(t.c.id == 2))
            assert not r.is_insert
            eq_(
                connection.execute(t.select().order_by(t.c.id)).fetchall(),
                [(1, "d1"), (3, "d3")],
            )

    class InsertBehaviorTest(_InsertBehaviorTest):
        @pytest.mark.skip(
            "BQ has no autoinc and client-side defaults can't work for select."
        )
        def test_insert_from_select_autoinc(cls):
            pass

        @pytest.mark.skip(
            "BQ has no autoinc and client-side defaults can't work for select."
        )
        def test_no_results_for_non_returning_insert(cls):
            pass

    # BQ has no autoinc and client-side defaults can't work for select
    del _IntegerTest.test_huge_int_auto_accommodation

    class NumericTest(_NumericTest):
        @testing.fixture
        def do_numeric_test(self, metadata, connection):
            def run(type_, input_, output, filter_=None, check_scale=False):
                t = Table("t", metadata, Column("x", type_))
                t.create(connection)
                connection.execute(t.insert(), [{"x": x} for x in input_])

                result = {row[0] for row in connection.execute(t.select())}
                output = set(output)
                if filter_:
                    result = {filter_(x) for x in result}
                    output = {filter_(x) for x in output}
                eq_(result, output)
                if check_scale:
                    eq_([str(x) for x in result], [str(x) for x in output])

                where_expr = True

                # Adding where clause for 2.0 compatibility
                connection.execute(t.delete().where(where_expr))

                # test that this is actually a number!
                # note we have tiny scale here as we have tests with very
                # small scale Numeric types.  PostgreSQL will raise an error
                # if you use values outside the available scale.
                if type_.asdecimal:
                    test_value = decimal.Decimal("2.9")
                    add_value = decimal.Decimal("37.12")
                else:
                    test_value = 2.9
                    add_value = 37.12

                connection.execute(t.insert(), {"x": test_value})
                assert_we_are_a_number = connection.scalar(
                    select(type_coerce(t.c.x + add_value, type_))
                )
                eq_(
                    round(assert_we_are_a_number, 3),
                    round(test_value + add_value, 3),
                )

            return run

    class DifficultParametersTest(_DifficultParametersTest):
        # removed parameters that dont work with bigquery
        tough_parameters = testing.combinations(
            ("boring",),
            ("per cent",),
            ("per % cent",),
            ("%percent",),
            ("col:ons",),
            ("_starts_with_underscore",),
            ("more :: %colons%",),
            ("_name",),
            ("___name",),
            ("42numbers",),
            ("percent%signs",),
            ("has spaces",),
            ("1param",),
            ("1col:on",),
            argnames="paramname",
        )

        @tough_parameters
        @config.requirements.unusual_column_name_characters
        def test_round_trip_same_named_column(self, paramname, connection, metadata):
            name = paramname

            t = Table(
                "t",
                metadata,
                Column("id", Integer, primary_key=True),
                Column(name, String(50), nullable=False),
            )

            # table is created
            t.create(connection)

            # automatic param generated by insert
            connection.execute(t.insert().values({"id": 1, name: "some name"}))

            # automatic param generated by criteria, plus selecting the column
            stmt = select(t.c[name]).where(t.c[name] == "some name")

            eq_(connection.scalar(stmt), "some name")

            # use the name in a param explicitly
            stmt = select(t.c[name]).where(t.c[name] == bindparam(name))

            row = connection.execute(stmt, {name: "some name"}).first()

            # name works as the key from cursor.description
            eq_(row._mapping[name], "some name")

            # use expanding IN
            stmt = select(t.c[name]).where(
                t.c[name].in_(["some name", "some other_name"])
            )

            row = connection.execute(stmt).first()

        @testing.fixture
        def multirow_fixture(self, metadata, connection):
            mytable = Table(
                "mytable",
                metadata,
                Column("myid", Integer),
                Column("name", String(50)),
                Column("desc", String(50)),
            )

            mytable.create(connection)

            connection.execute(
                mytable.insert(),
                [
                    {"myid": 1, "name": "a", "desc": "a_desc"},
                    {"myid": 2, "name": "b", "desc": "b_desc"},
                    {"myid": 3, "name": "c", "desc": "c_desc"},
                    {"myid": 4, "name": "d", "desc": "d_desc"},
                ],
            )
            yield mytable

        @tough_parameters
        def test_standalone_bindparam_escape(
            self, paramname, connection, multirow_fixture
        ):
            tbl1 = multirow_fixture
            stmt = select(tbl1.c.myid).where(
                tbl1.c.name == bindparam(paramname, value="x")
            )
            res = connection.scalar(stmt, {paramname: "c"})
            eq_(res, 3)

        @tough_parameters
        def test_standalone_bindparam_escape_expanding(
            self, paramname, connection, multirow_fixture
        ):
            tbl1 = multirow_fixture
            stmt = (
                select(tbl1.c.myid)
                .where(tbl1.c.name.in_(bindparam(paramname, value=["a", "b"])))
                .order_by(tbl1.c.myid)
            )

            res = connection.scalars(stmt, {paramname: ["d", "a"]}).all()
            eq_(res, [1, 4])

    class FetchLimitOffsetTest(_FetchLimitOffsetTest):
        @pytest.mark.skip("BigQuery doesn't allow an offset without a limit.")
        def test_simple_offset(self):
            pass

        test_bound_offset = test_simple_offset
        test_expr_offset = test_simple_offset_zero = test_simple_offset
        test_limit_offset_nobinds = test_simple_offset  # TODO figure out
        # how to prevent this from failing
        # The original test is missing an order by.

        # The original test is missing an order by.

        # Also, note that sqlalchemy union is a union distinct, not a
        # union all. This test caught that were were getting that wrong.
        def test_limit_render_multiple_times(self, connection):
            table = self.tables.some_table
            stmt = select(table.c.id).order_by(table.c.id).limit(1).scalar_subquery()

            u = sqlalchemy.union(select(stmt), select(stmt)).subquery().select()

            self._assert_result(
                connection,
                u,
                [(1,)],
            )

    class UuidTest(_UuidTest):
        @classmethod
        def define_tables(cls, metadata):
            Table(
                "uuid_table",
                metadata,
                Column("id", Integer, primary_key=True, test_needs_autoincrement=True),
                Column("uuid_data", String),  # Use native UUID for primary data
                Column(
                    "uuid_text_data", String, nullable=True
                ),  # Optional text representation
                Column("uuid_data_nonnative", String),
                Column("uuid_text_data_nonnative", String),
            )

        def test_uuid_round_trip(self, connection):
            data = str(uuid.uuid4())
            uuid_table = self.tables.uuid_table

            connection.execute(
                uuid_table.insert(),
                {"id": 1, "uuid_data": data, "uuid_data_nonnative": data},
            )
            row = connection.execute(
                select(uuid_table.c.uuid_data, uuid_table.c.uuid_data_nonnative).where(
                    uuid_table.c.uuid_data == data,
                    uuid_table.c.uuid_data_nonnative == data,
                )
            ).first()
            eq_(row, (data, data))

        def test_uuid_text_round_trip(self, connection):
            data = str(uuid.uuid4())
            uuid_table = self.tables.uuid_table

            connection.execute(
                uuid_table.insert(),
                {
                    "id": 1,
                    "uuid_text_data": data,
                    "uuid_text_data_nonnative": data,
                },
            )
            row = connection.execute(
                select(
                    uuid_table.c.uuid_text_data,
                    uuid_table.c.uuid_text_data_nonnative,
                ).where(
                    uuid_table.c.uuid_text_data == data,
                    uuid_table.c.uuid_text_data_nonnative == data,
                )
            ).first()
            eq_((row[0].lower(), row[1].lower()), (data, data))

        def test_literal_uuid(self, literal_round_trip):
            data = str(uuid.uuid4())
            literal_round_trip(String(), [data], [data])

        def test_literal_text(self, literal_round_trip):
            data = str(uuid.uuid4())
            literal_round_trip(
                String(),
                [data],
                [data],
                filter_=lambda x: x.lower(),
            )

        def test_literal_nonnative_uuid(self, literal_round_trip):
            data = str(uuid.uuid4())
            literal_round_trip(String(), [data], [data])

        def test_literal_nonnative_text(self, literal_round_trip):
            data = str(uuid.uuid4())
            literal_round_trip(
                String(),
                [data],
                [data],
                filter_=lambda x: x.lower(),
            )

        @testing.requires.insert_returning
        def test_uuid_returning(self, connection):
            data = str(uuid.uuid4())
            str_data = str(data)
            uuid_table = self.tables.uuid_table

            result = connection.execute(
                uuid_table.insert().returning(
                    uuid_table.c.uuid_data,
                    uuid_table.c.uuid_text_data,
                    uuid_table.c.uuid_data_nonnative,
                    uuid_table.c.uuid_text_data_nonnative,
                ),
                {
                    "id": 1,
                    "uuid_data": data,
                    "uuid_text_data": str_data,
                    "uuid_data_nonnative": data,
                    "uuid_text_data_nonnative": str_data,
                },
            )
            row = result.first()

            eq_(row, (data, str_data, data, str_data))

    class StringTest(_StringTest):
        def test_dont_truncate_rightside(
            self, metadata, connection, expr=None, expected=None
        ):
            t = Table(
                "t",
                metadata,
                Column("x", String(2)),
                Column("id", Integer, primary_key=True),
            )
            t.create(connection)
            connection.connection.commit()
            connection.execute(
                t.insert(),
                [{"x": "AB", "id": 1}, {"x": "BC", "id": 2}, {"x": "AC", "id": 3}],
            )
            combinations = [("%B%", ["AB", "BC"]), ("A%C", ["AC"]), ("A%C%Z", [])]

            for args in combinations:
                eq_(
                    connection.scalars(select(t.c.x).where(t.c.x.like(args[0]))).all(),
                    args[1],
                )

    # from else statement ....
    del DistinctOnTest  # expects unquoted table names.
    del HasIndexTest  # BQ doesn't do the indexes that SQLA is loooking for.
    del IdentityAutoincrementTest  # BQ doesn't do autoincrement
    del PostCompileParamsTest  # BQ adds backticks to bind parameters, causing failure of tests TODO: fix this?

else:
    from sqlalchemy.testing.suite import (
        FetchLimitOffsetTest as _FetchLimitOffsetTest,
        RowCountTest as _RowCountTest,
    )

    class FetchLimitOffsetTest(_FetchLimitOffsetTest):
        @pytest.mark.skip("BigQuery doesn't allow an offset without a limit.")
        def test_simple_offset(self):
            pass

        test_bound_offset = test_simple_offset
        test_expr_offset = test_simple_offset_zero = test_simple_offset
        test_limit_offset_nobinds = test_simple_offset  # TODO figure out
        # how to prevent this from failing
        # The original test is missing an order by.

        # Also, note that sqlalchemy union is a union distinct, not a
        # union all. This test caught that were were getting that wrong.
        def test_limit_render_multiple_times(self, connection):
            table = self.tables.some_table
            stmt = select(table.c.id).order_by(table.c.id).limit(1).scalar_subquery()

            u = sqlalchemy.union(select(stmt), select(stmt)).subquery().select()

            self._assert_result(
                connection,
                u,
                [(1,)],
            )

    del DifficultParametersTest  # exercises column names illegal in BQ
    del DistinctOnTest  # expects unquoted table names.
    del HasIndexTest  # BQ doesn't do the indexes that SQLA is loooking for.
    del IdentityAutoincrementTest  # BQ doesn't do autoincrement

    # This test makes makes assertions about generated sql and trips
    # over the backquotes that we add everywhere. XXX Why do we do that?
    del PostCompileParamsTest

    class TimestampMicrosecondsTest(_TimestampMicrosecondsTest):
        data = datetime.datetime(2012, 10, 15, 12, 57, 18, 396, tzinfo=pytz.UTC)

        def test_literal(self, literal_round_trip):
            # The base tests doesn't set up the literal properly, because
            # it doesn't pass its datatype to `literal`.

            def literal(value, type_=None):
                assert value == self.data
                if type_ is not None:
                    assert type_ is self.datatype

                return sqlalchemy.sql.elements.literal(value, self.datatype)

            with mock.patch("sqlalchemy.testing.suite.test_types.literal", literal):
                super(TimestampMicrosecondsTest, self).test_literal(literal_round_trip)

        def test_select_direct(self, connection):
            # This func added because this test was failing when passed the
            # UTC timezone.

            def literal(value, type_=None):
                assert value == self.data

                if type_ is not None:
                    assert type_ is self.datatype

                import sqlalchemy.sql.sqltypes

                return sqlalchemy.sql.elements.literal(value, self.datatype)

            with mock.patch("sqlalchemy.testing.suite.test_types.literal", literal):
                super(TimestampMicrosecondsTest, self).test_select_direct(connection)

    def test_round_trip_executemany(self, connection):
        unicode_table = self.tables.unicode_table
        connection.execute(
            unicode_table.insert(),
            [{"id": i, "unicode_data": self.data} for i in range(3)],
        )

        rows = connection.execute(select(unicode_table.c.unicode_data)).fetchall()
        eq_(rows, [(self.data,) for i in range(3)])
        for row in rows:
            assert isinstance(row[0], util.text_type)

    sqlalchemy.testing.suite.test_types._UnicodeFixture.test_round_trip_executemany = (
        test_round_trip_executemany
    )

    class RowCountTest(_RowCountTest):
        @classmethod
        def insert_data(cls, connection):
            cls.data = data = [
                ("Angela", "A"),
                ("Andrew", "A"),
                ("Anand", "A"),
                ("Bob", "B"),
                ("Bobette", "B"),
                ("Buffy", "B"),
                ("Charlie", "C"),
                ("Cynthia", "C"),
                ("Chris", "C"),
            ]

            employees_table = cls.tables.employees
            connection.execute(
                employees_table.insert(),
                [
                    {"employee_id": i, "name": n, "department": d}
                    for i, (n, d) in enumerate(data)
                ],
            )

    class InsertBehaviorTest(_InsertBehaviorTest):
        @pytest.mark.skip(
            "BQ has no autoinc and client-side defaults can't work for select."
        )
        def test_insert_from_select_autoinc(cls):
            pass

    class SimpleUpdateDeleteTest(_SimpleUpdateDeleteTest):
        """The base tests fail if operations return rows for some reason."""

        def test_update(self):
            t = self.tables.plain_pk
            r = config.db.execute(t.update().where(t.c.id == 2), data="d2_new")
            assert not r.is_insert

            eq_(
                config.db.execute(t.select().order_by(t.c.id)).fetchall(),
                [(1, "d1"), (2, "d2_new"), (3, "d3")],
            )

        def test_delete(self):
            t = self.tables.plain_pk
            r = config.db.execute(t.delete().where(t.c.id == 2))
            assert not r.is_insert
            eq_(
                config.db.execute(t.select().order_by(t.c.id)).fetchall(),
                [(1, "d1"), (3, "d3")],
            )


# Quotes aren't allowed in BigQuery table names.
del QuotedNameArgumentTest


# class InsertBehaviorTest(_InsertBehaviorTest):
#     @pytest.mark.skip(
#         "BQ has no autoinc and client-side defaults can't work for select."
#     )
#     def test_insert_from_select_autoinc(cls):
#         pass


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
                select(stuff.c.id).where(
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
                select(stuff.c.id).where(exists().where(stuff.c.data == "no data"))
            ).fetchall(),
            [],
        )


# This test requires features (indexes, primary keys, etc., that BigQuery doesn't have.
del LongNameBlowoutTest


# class SimpleUpdateDeleteTest(_SimpleUpdateDeleteTest):
#     """The base tests fail if operations return rows for some reason."""

#     def test_update(self):
#         t = self.tables.plain_pk
#         r = config.db.execute(t.update().where(t.c.id == 2), data="d2_new")
#         assert not r.is_insert
#         # assert not r.returns_rows

#         eq_(
#             config.db.execute(t.select().order_by(t.c.id)).fetchall(),
#             [(1, "d1"), (2, "d2_new"), (3, "d3")],
#         )

#     def test_delete(self):
#         t = self.tables.plain_pk
#         r = config.db.execute(t.delete().where(t.c.id == 2))
#         assert not r.is_insert
#         # assert not r.returns_rows
#         eq_(
#             config.db.execute(t.select().order_by(t.c.id)).fetchall(),
#             [(1, "d1"), (3, "d3")],
#         )


class CTETest(_CTETest):
    @pytest.mark.skip("Can't use CTEs with insert")
    def test_insert_from_select_round_trip(self):
        pass

    @pytest.mark.skip("Recusive CTEs aren't supported.")
    def test_select_recursive_round_trip(self):
        pass


del ComponentReflectionTest  # Multiple tests re: CHECK CONSTRAINTS, etc which
# BQ does not support
# class ComponentReflectionTest(_ComponentReflectionTest):
#     @pytest.mark.skip("Big query types don't track precision, length, etc.")
#     def course_grained_types():
#         pass

#     test_numeric_reflection = test_varchar_reflection = course_grained_types

#     @pytest.mark.skip("BQ doesn't have indexes (in the way these tests expect).")
#     def test_get_indexes(self):
#         pass

del ArrayTest  # only appears to apply to postgresql
del BizarroCharacterFKResolutionTest
del HasTableTest.test_has_table_cache  # TODO confirm whether BQ has table caching
