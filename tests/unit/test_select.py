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
from decimal import Decimal

import packaging.version
import pytest
import sqlalchemy

import sqlalchemy_bigquery

from .conftest import (
    setup_table,
    sqlalchemy_version,
    sqlalchemy_1_3_or_higher,
    sqlalchemy_1_4_or_higher,
    sqlalchemy_before_1_4,
)


def test_labels_not_forced(faux_conn):
    table = setup_table(faux_conn, "t", sqlalchemy.Column("id", sqlalchemy.Integer))
    result = faux_conn.execute(sqlalchemy.select([table.c.id]))
    assert result.keys() == ["id"]  # Look! Just the column name!


def dtrepr(v):
    return f"{v.__class__.__name__.upper()} {repr(str(v))}"


@pytest.mark.parametrize(
    "type_,val,btype,vrep",
    [
        (sqlalchemy.String, "myString", "STRING", repr),
        (sqlalchemy.Text, "myText", "STRING", repr),
        (sqlalchemy.Unicode, "myUnicode", "STRING", repr),
        (sqlalchemy.UnicodeText, "myUnicodeText", "STRING", repr),
        (sqlalchemy.Integer, 424242, "INT64", repr),
        (sqlalchemy.SmallInteger, 42, "INT64", repr),
        (sqlalchemy.BigInteger, 1 << 60, "INT64", repr),
        (sqlalchemy.Numeric, Decimal(42), "NUMERIC", str),
        (sqlalchemy.Float, 4.2, "FLOAT64", repr),
        (
            sqlalchemy.DateTime,
            datetime.datetime(2021, 2, 3, 4, 5, 6, 123456),
            "DATETIME",
            dtrepr,
        ),
        (sqlalchemy.Date, datetime.date(2021, 2, 3), "DATE", dtrepr),
        (sqlalchemy.Time, datetime.time(4, 5, 6, 123456), "TIME", dtrepr),
        (sqlalchemy.Boolean, True, "BOOL", "true"),
        (sqlalchemy.REAL, 1.42, "FLOAT64", repr),
        (sqlalchemy.FLOAT, 0.42, "FLOAT64", repr),
        (sqlalchemy.NUMERIC, Decimal(4.25), "NUMERIC", str),
        (sqlalchemy.NUMERIC(39), Decimal(4.25), "BIGNUMERIC(39)", str),
        (sqlalchemy.NUMERIC(30, 10), Decimal(4.25), "BIGNUMERIC(30, 10)", str),
        (sqlalchemy.NUMERIC(39, 10), Decimal(4.25), "BIGNUMERIC(39, 10)", str),
        (sqlalchemy.DECIMAL, Decimal(0.25), "NUMERIC", str),
        (sqlalchemy.DECIMAL(39), Decimal(4.25), "BIGNUMERIC(39)", str),
        (sqlalchemy.DECIMAL(30, 10), Decimal(4.25), "BIGNUMERIC(30, 10)", str),
        (sqlalchemy.DECIMAL(39, 10), Decimal(4.25), "BIGNUMERIC(39, 10)", str),
        (sqlalchemy.INTEGER, 434343, "INT64", repr),
        (sqlalchemy.INT, 444444, "INT64", repr),
        (sqlalchemy.SMALLINT, 43, "INT64", repr),
        (sqlalchemy.BIGINT, 1 << 61, "INT64", repr),
        (
            sqlalchemy.TIMESTAMP,
            datetime.datetime(2021, 2, 3, 4, 5, 7, 123456),
            "TIMESTAMP",
            lambda v: f"TIMESTAMP {repr(str(v))}",
        ),
        (
            sqlalchemy.DATETIME,
            datetime.datetime(2021, 2, 3, 4, 5, 8, 123456),
            "DATETIME",
            dtrepr,
        ),
        (sqlalchemy.DATE, datetime.date(2021, 2, 4), "DATE", dtrepr),
        (sqlalchemy.TIME, datetime.time(4, 5, 7, 123456), "TIME", dtrepr),
        (sqlalchemy.TIME, datetime.time(4, 5, 7), "TIME", dtrepr),
        (sqlalchemy.TEXT, "myTEXT", "STRING", repr),
        (sqlalchemy.VARCHAR, "myVARCHAR", "STRING", repr),
        (sqlalchemy.NVARCHAR, "myNVARCHAR", "STRING", repr),
        (sqlalchemy.VARCHAR(42), "myVARCHAR", "STRING(42)", repr),
        (sqlalchemy.NVARCHAR(42), "myNVARCHAR", "STRING(42)", repr),
        (sqlalchemy.CHAR, "myCHAR", "STRING", repr),
        (sqlalchemy.NCHAR, "myNCHAR", "STRING", repr),
        (sqlalchemy.BINARY, b"myBINARY", "BYTES", repr),
        (sqlalchemy.VARBINARY, b"myVARBINARY", "BYTES", repr),
        (sqlalchemy.VARBINARY(42), b"myVARBINARY", "BYTES(42)", repr),
        (sqlalchemy.BOOLEAN, False, "BOOL", "false"),
        (sqlalchemy.ARRAY(sqlalchemy.Integer), [1, 2, 3], "ARRAY<INT64>", repr),
        (
            sqlalchemy.ARRAY(sqlalchemy.DATETIME),
            [
                datetime.datetime(2021, 2, 3, 4, 5, 6),
                datetime.datetime(2021, 2, 3, 4, 5, 7, 123456),
                datetime.datetime(2021, 2, 3, 4, 5, 8, 123456),
            ],
            "ARRAY<DATETIME>",
            lambda a: "[" + ", ".join(dtrepr(v) for v in a) + "]",
        ),
    ],
)
def test_typed_parameters(faux_conn, type_, val, btype, vrep):
    col_name = "foo"
    table = setup_table(faux_conn, "t", sqlalchemy.Column(col_name, type_))

    assert faux_conn.test_data["execute"].pop()[0].strip() == (
        f"CREATE TABLE `t` (\n" f"\t`{col_name}` {btype}\n" f")"
    )

    faux_conn.execute(table.insert().values(**{col_name: val}))

    ptype = btype[: btype.index("(")] if "(" in btype else btype

    assert faux_conn.test_data["execute"][-1] == (
        f"INSERT INTO `t` (`{col_name}`) VALUES (%({col_name}:{ptype})s)",
        {col_name: val},
    )

    faux_conn.execute(
        table.insert()
        .values(**{col_name: sqlalchemy.literal(val, type_)})
        .compile(
            dialect=sqlalchemy_bigquery.BigQueryDialect(),
            compile_kwargs=dict(literal_binds=True),
        )
    )

    if not isinstance(vrep, str):
        vrep = vrep(val)

    assert faux_conn.test_data["execute"][-1] == (
        f"INSERT INTO `t` (`{col_name}`) VALUES ({vrep})",
        {},
    )

    assert list(map(list, faux_conn.execute(sqlalchemy.select([table])))) == [[val]] * 2
    assert faux_conn.test_data["execute"][-1][0] == "SELECT `t`.`foo` \nFROM `t`"

    assert (
        list(
            map(
                list,
                faux_conn.execute(sqlalchemy.select([table.c.foo], use_labels=True)),
            )
        )
        == [[val]] * 2
    )
    assert faux_conn.test_data["execute"][-1][0] == (
        "SELECT `t`.`foo` AS `t_foo` \nFROM `t`"
    )


def test_select_struct(faux_conn, metadata):
    from sqlalchemy_bigquery import STRUCT

    table = sqlalchemy.Table(
        "t",
        metadata,
        sqlalchemy.Column("x", STRUCT(y=sqlalchemy.Integer)),
    )

    faux_conn.ex("create table t (x RECORD)")
    faux_conn.ex("""insert into t values ('{"y": 1}')""")

    row = list(faux_conn.execute(sqlalchemy.select([table])))[0]
    # We expect the raw string, because sqlite3, unlike BigQuery
    # doesn't deserialize for us.
    assert row.x == '{"y": 1}'


def test_select_label_starts_w_digit(faux_conn):
    # Make sure label names are legal identifiers
    faux_conn.execute(sqlalchemy.select([sqlalchemy.literal(1).label("2foo")]))
    assert (
        faux_conn.test_data["execute"][-1][0] == "SELECT %(param_1:INT64)s AS `_2foo`"
    )


def test_force_quote(faux_conn):
    from sqlalchemy.sql.elements import quoted_name

    table = setup_table(
        faux_conn,
        "t",
        sqlalchemy.Column(quoted_name("foo", True), sqlalchemy.Integer),
    )
    faux_conn.execute(sqlalchemy.select([table]))
    assert faux_conn.test_data["execute"][-1][0] == ("SELECT `t`.`foo` \nFROM `t`")


def test_disable_quote(faux_conn):
    from sqlalchemy.sql.elements import quoted_name

    table = setup_table(
        faux_conn,
        "t",
        sqlalchemy.Column(quoted_name("foo", False), sqlalchemy.Integer),
    )
    faux_conn.execute(sqlalchemy.select([table]))
    assert faux_conn.test_data["execute"][-1][0] == ("SELECT `t`.foo \nFROM `t`")


@sqlalchemy_before_1_4
def test_select_in_lit_13(faux_conn):
    [[isin]] = faux_conn.execute(
        sqlalchemy.select([sqlalchemy.literal(1).in_([1, 2, 3])])
    )
    assert isin
    assert faux_conn.test_data["execute"][-1] == (
        "SELECT %(param_1:INT64)s IN "
        "(%(param_2:INT64)s, %(param_3:INT64)s, %(param_4:INT64)s) AS `anon_1`",
        {"param_1": 1, "param_2": 1, "param_3": 2, "param_4": 3},
    )


@sqlalchemy_1_4_or_higher
def test_select_in_lit(faux_conn, last_query):
    faux_conn.execute(sqlalchemy.select([sqlalchemy.literal(1).in_([1, 2, 3])]))
    last_query(
        "SELECT %(param_1:INT64)s IN UNNEST(%(param_2:INT64)s) AS `anon_1`",
        {"param_1": 1, "param_2": [1, 2, 3]},
    )


def test_select_in_param(faux_conn, last_query):
    [[isin]] = faux_conn.execute(
        sqlalchemy.select(
            [sqlalchemy.literal(1).in_(sqlalchemy.bindparam("q", expanding=True))]
        ),
        dict(q=[1, 2, 3]),
    )
    if sqlalchemy_version >= packaging.version.parse("1.4"):
        last_query(
            "SELECT %(param_1:INT64)s IN UNNEST(%(q:INT64)s) AS `anon_1`",
            {"param_1": 1, "q": [1, 2, 3]},
        )
    else:
        assert isin
        last_query(
            "SELECT %(param_1:INT64)s IN UNNEST("
            "[ %(q_1:INT64)s, %(q_2:INT64)s, %(q_3:INT64)s ]"
            ") AS `anon_1`",
            {"param_1": 1, "q_1": 1, "q_2": 2, "q_3": 3},
        )


def test_select_in_param1(faux_conn, last_query):
    [[isin]] = faux_conn.execute(
        sqlalchemy.select(
            [sqlalchemy.literal(1).in_(sqlalchemy.bindparam("q", expanding=True))]
        ),
        dict(q=[1]),
    )
    if sqlalchemy_version >= packaging.version.parse("1.4"):
        last_query(
            "SELECT %(param_1:INT64)s IN UNNEST(%(q:INT64)s) AS `anon_1`",
            {"param_1": 1, "q": [1]},
        )
    else:
        assert isin
        last_query(
            "SELECT %(param_1:INT64)s IN UNNEST(" "[ %(q_1:INT64)s ]" ") AS `anon_1`",
            {"param_1": 1, "q_1": 1},
        )


@sqlalchemy_1_3_or_higher
def test_select_in_param_empty(faux_conn, last_query):
    [[isin]] = faux_conn.execute(
        sqlalchemy.select(
            [sqlalchemy.literal(1).in_(sqlalchemy.bindparam("q", expanding=True))]
        ),
        dict(q=[]),
    )
    if sqlalchemy_version >= packaging.version.parse("1.4"):
        last_query(
            "SELECT %(param_1:INT64)s IN UNNEST(%(q:INT64)s) AS `anon_1`",
            {"param_1": 1, "q": []},
        )
    else:
        assert not isin
        last_query(
            "SELECT %(param_1:INT64)s IN UNNEST([  ]) AS `anon_1`", {"param_1": 1}
        )


@sqlalchemy_before_1_4
def test_select_notin_lit13(faux_conn):
    [[isnotin]] = faux_conn.execute(
        sqlalchemy.select([sqlalchemy.literal(0).notin_([1, 2, 3])])
    )
    assert isnotin
    assert faux_conn.test_data["execute"][-1] == (
        "SELECT (%(param_1:INT64)s NOT IN "
        "(%(param_2:INT64)s, %(param_3:INT64)s, %(param_4:INT64)s)) AS `anon_1`",
        {"param_1": 0, "param_2": 1, "param_3": 2, "param_4": 3},
    )


@sqlalchemy_1_4_or_higher
def test_select_notin_lit(faux_conn, last_query):
    faux_conn.execute(sqlalchemy.select([sqlalchemy.literal(0).notin_([1, 2, 3])]))
    last_query(
        "SELECT (%(param_1:INT64)s NOT IN UNNEST(%(param_2:INT64)s)) AS `anon_1`",
        {"param_1": 0, "param_2": [1, 2, 3]},
    )


def test_select_notin_param(faux_conn, last_query):
    [[isnotin]] = faux_conn.execute(
        sqlalchemy.select(
            [sqlalchemy.literal(1).notin_(sqlalchemy.bindparam("q", expanding=True))]
        ),
        dict(q=[1, 2, 3]),
    )
    if sqlalchemy_version >= packaging.version.parse("1.4"):
        last_query(
            "SELECT (%(param_1:INT64)s NOT IN UNNEST(%(q:INT64)s)) AS `anon_1`",
            {"param_1": 1, "q": [1, 2, 3]},
        )
    else:
        assert not isnotin
        last_query(
            "SELECT (%(param_1:INT64)s NOT IN UNNEST("
            "[ %(q_1:INT64)s, %(q_2:INT64)s, %(q_3:INT64)s ]"
            ")) AS `anon_1`",
            {"param_1": 1, "q_1": 1, "q_2": 2, "q_3": 3},
        )


@sqlalchemy_1_3_or_higher
def test_select_notin_param_empty(faux_conn, last_query):
    [[isnotin]] = faux_conn.execute(
        sqlalchemy.select(
            [sqlalchemy.literal(1).notin_(sqlalchemy.bindparam("q", expanding=True))]
        ),
        dict(q=[]),
    )
    if sqlalchemy_version >= packaging.version.parse("1.4"):
        last_query(
            "SELECT (%(param_1:INT64)s NOT IN UNNEST(%(q:INT64)s)) AS `anon_1`",
            {"param_1": 1, "q": []},
        )
    else:
        assert isnotin
        last_query(
            "SELECT (%(param_1:INT64)s NOT IN UNNEST([  ])) AS `anon_1`", {"param_1": 1}
        )


def test_literal_binds_kwarg_with_an_IN_operator_252(faux_conn):
    table = setup_table(
        faux_conn,
        "test",
        sqlalchemy.Column("val", sqlalchemy.Integer),
        initial_data=[dict(val=i) for i in range(3)],
    )
    q = sqlalchemy.select([table.c.val]).where(table.c.val.in_([2]))

    def nstr(q):
        return " ".join(str(q).strip().split())

    assert (
        nstr(q.compile(faux_conn.engine, compile_kwargs={"literal_binds": True}))
        == "SELECT `test`.`val` FROM `test` WHERE `test`.`val` IN (2)"
    )


@sqlalchemy_1_4_or_higher
@pytest.mark.parametrize("alias", [True, False])
def test_unnest(faux_conn, alias):
    from sqlalchemy import String
    from sqlalchemy_bigquery import ARRAY

    table = setup_table(faux_conn, "t", sqlalchemy.Column("objects", ARRAY(String)))
    fcall = sqlalchemy.func.unnest(table.c.objects)
    if alias:
        query = fcall.alias("foo_objects").column
    else:
        query = fcall.column_valued("foo_objects")
    compiled = str(sqlalchemy.select(query).compile(faux_conn.engine))
    assert " ".join(compiled.strip().split()) == (
        "SELECT `foo_objects` FROM `t` `t_1`, unnest(`t_1`.`objects`) AS `foo_objects`"
    )


@sqlalchemy_1_4_or_higher
@pytest.mark.parametrize("alias", [True, False])
def test_table_valued_alias_w_multiple_references_to_the_same_table(faux_conn, alias):
    from sqlalchemy import String
    from sqlalchemy_bigquery import ARRAY

    table = setup_table(faux_conn, "t", sqlalchemy.Column("objects", ARRAY(String)))
    fcall = sqlalchemy.func.foo(table.c.objects, table.c.objects)
    if alias:
        query = fcall.alias("foo_objects").column
    else:
        query = fcall.column_valued("foo_objects")
    compiled = str(sqlalchemy.select(query).compile(faux_conn.engine))
    assert " ".join(compiled.strip().split()) == (
        "SELECT `foo_objects` "
        "FROM `t` `t_1`, foo(`t_1`.`objects`, `t_1`.`objects`) AS `foo_objects`"
    )


@sqlalchemy_1_4_or_higher
@pytest.mark.parametrize("alias", [True, False])
def test_unnest_w_no_table_references(faux_conn, alias):
    fcall = sqlalchemy.func.unnest([1, 2, 3])
    if alias:
        query = fcall.alias().column
    else:
        query = fcall.column_valued()
    compiled = str(sqlalchemy.select(query).compile(faux_conn.engine))
    assert " ".join(compiled.strip().split()) == (
        "SELECT `anon_1` FROM unnest(%(unnest_1)s) AS `anon_1`"
    )


def test_array_indexing(faux_conn, metadata):
    t = sqlalchemy.Table(
        "t",
        metadata,
        sqlalchemy.Column("a", sqlalchemy.ARRAY(sqlalchemy.String)),
    )
    got = str(sqlalchemy.select([t.c.a[0]]).compile(faux_conn.engine))
    assert got == "SELECT `t`.`a`[OFFSET(%(a_1:INT64)s)] AS `anon_1` \nFROM `t`"
