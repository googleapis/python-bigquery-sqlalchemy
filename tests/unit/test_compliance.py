# Copyright 2021 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""
Ported compliance tests.

Mainly to get better unit test coverage.
"""

import pytest
import sqlalchemy
from sqlalchemy import Column, Integer, literal_column, select, String, Table, union
from sqlalchemy.testing.assertions import eq_, in_

from conftest import setup_table, sqlalchemy_1_3_or_higher


def assert_result(connection, sel, expected):
    eq_(connection.execute(sel).fetchall(), expected)


def some_table(connection):
    return setup_table(
        connection,
        "some_table",
        Column("id", Integer),
        Column("x", Integer),
        Column("y", Integer),
        initial_data=[
            {"id": 1, "x": 1, "y": 2},
            {"id": 2, "x": 2, "y": 3},
            {"id": 3, "x": 3, "y": 4},
            {"id": 4, "x": 4, "y": 5},
        ],
    )


def test_distinct_selectable_in_unions(faux_conn):
    table = some_table(faux_conn)
    s1 = select([table]).where(table.c.id == 2).distinct()
    s2 = select([table]).where(table.c.id == 3).distinct()

    u1 = union(s1, s2).limit(2)
    assert_result(faux_conn, u1.order_by(u1.c.id), [(2, 2, 3), (3, 3, 4)])


def test_limit_offset_aliased_selectable_in_unions(faux_conn):
    table = some_table(faux_conn)
    s1 = (
        select([table])
        .where(table.c.id == 2)
        .limit(1)
        .order_by(table.c.id)
        .alias()
        .select()
    )
    s2 = (
        select([table])
        .where(table.c.id == 3)
        .limit(1)
        .order_by(table.c.id)
        .alias()
        .select()
    )

    u1 = union(s1, s2).limit(2)
    assert_result(faux_conn, u1.order_by(u1.c.id), [(2, 2, 3), (3, 3, 4)])


def test_percent_sign_round_trip(faux_conn, metadata):
    """test that the DBAPI accommodates for escaped / nonescaped
    percent signs in a way that matches the compiler

    """
    t = Table("t", metadata, Column("data", String(50)))
    t.create(faux_conn.engine)
    faux_conn.execute(t.insert(), dict(data="some % value"))
    faux_conn.execute(t.insert(), dict(data="some %% other value"))
    eq_(
        faux_conn.scalar(
            select([t.c.data]).where(t.c.data == literal_column("'some % value'"))
        ),
        "some % value",
    )

    eq_(
        faux_conn.scalar(
            select([t.c.data]).where(
                t.c.data == literal_column("'some %% other value'")
            )
        ),
        "some %% other value",
    )


@sqlalchemy_1_3_or_higher
def test_null_in_empty_set_is_false(faux_conn):
    stmt = select(
        [
            sqlalchemy.case(
                [
                    (
                        sqlalchemy.null().in_(
                            sqlalchemy.bindparam("foo", value=(), expanding=True)
                        ),
                        sqlalchemy.true(),
                    )
                ],
                else_=sqlalchemy.false(),
            )
        ]
    )
    in_(faux_conn.execute(stmt).fetchone()[0], (False, 0))


@pytest.mark.parametrize(
    "meth,arg,expected",
    [
        ("contains", "b%cde", {1, 2, 3, 4, 5, 6, 7, 8, 9}),
        ("startswith", "ab%c", {1, 2, 3, 4, 5, 6, 7, 8, 9, 10}),
        ("endswith", "e%fg", {1, 2, 3, 4, 5, 6, 7, 8, 9}),
    ],
)
def test_likish(faux_conn, meth, arg, expected):
    # See sqlalchemy.testing.suite.test_select.LikeFunctionsTest
    table = setup_table(
        faux_conn,
        "t",
        Column("id", Integer, primary_key=True),
        Column("data", String(50)),
        initial_data=[
            {"id": 1, "data": "abcdefg"},
            {"id": 2, "data": "ab/cdefg"},
            {"id": 3, "data": "ab%cdefg"},
            {"id": 4, "data": "ab_cdefg"},
            {"id": 5, "data": "abcde/fg"},
            {"id": 6, "data": "abcde%fg"},
            {"id": 7, "data": "ab#cdefg"},
            {"id": 8, "data": "ab9cdefg"},
            {"id": 9, "data": "abcde#fg"},
            {"id": 10, "data": "abcd9fg"},
        ],
    )
    expr = getattr(table.c.data, meth)(arg)
    rows = {value for value, in faux_conn.execute(select([table.c.id]).where(expr))}
    eq_(rows, expected)

    all = {i for i in range(1, 11)}
    expr = sqlalchemy.not_(expr)
    rows = {value for value, in faux_conn.execute(select([table.c.id]).where(expr))}
    eq_(rows, all - expected)


def test_group_by_composed(faux_conn):
    table = setup_table(
        faux_conn,
        "t",
        Column("id", Integer, primary_key=True),
        Column("x", Integer),
        Column("y", Integer),
        Column("q", String(50)),
        Column("p", String(50)),
        initial_data=[
            {"id": 1, "x": 1, "y": 2, "q": "q1", "p": "p3"},
            {"id": 2, "x": 2, "y": 3, "q": "q2", "p": "p2"},
            {"id": 3, "x": 3, "y": 4, "q": "q3", "p": "p1"},
        ],
    )

    expr = (table.c.x + table.c.y).label("lx")
    stmt = (
        select([sqlalchemy.func.count(table.c.id), expr]).group_by(expr).order_by(expr)
    )
    assert_result(faux_conn, stmt, [(1, 3), (1, 5), (1, 7)])
