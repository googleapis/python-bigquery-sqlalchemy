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
from sqlalchemy import Column, Integer, select, union
from sqlalchemy.testing.assertions import eq_

def setup_table(connection, name, *columns, initial_data=(), **kw):
    metadata = sqlalchemy.MetaData()
    table = sqlalchemy.Table(name, metadata, *columns, **kw)
    metadata.create_all(connection.engine)
    if initial_data:
        connection.execute(table.insert(), initial_data)
    return table

def assert_result(connection, sel, expected):
    eq_(connection.execute(sel).fetchall(), expected)


def test_distinct_selectable_in_unions(faux_conn):
    table = setup_table(
        faux_conn,
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
    s1 = select([table]).where(table.c.id == 2).distinct()
    s2 = select([table]).where(table.c.id == 3).distinct()

    u1 = union(s1, s2).limit(2)
    assert_result(faux_conn, u1.order_by(u1.c.id), [(2, 2, 3), (3, 3, 4)])
