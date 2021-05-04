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

import sqlalchemy
from sqlalchemy import Column, Integer, literal_column, select, String, Table, union
from sqlalchemy.testing.assertions import eq_

def setup_table(connection, name, *columns, initial_data=(), **kw):
    metadata = sqlalchemy.MetaData()
    table = Table(name, metadata, *columns, **kw)
    metadata.create_all(connection.engine)
    if initial_data:
        connection.execute(table.insert(), initial_data)
    return table

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
            ]
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
            select([t.c.data]).where(
                t.c.data == literal_column("'some % value'")
                )
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
