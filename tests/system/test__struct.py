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

import sqlalchemy

import sqlalchemy_bigquery


def test_struct(engine, bigquery_dataset, metadata):
    conn = engine.connect()
    table = sqlalchemy.Table(
        f"{bigquery_dataset}.test_struct",
        metadata,
        sqlalchemy.Column(
            "person",
            sqlalchemy_bigquery.STRUCT(
                name=sqlalchemy.String,
                children=sqlalchemy.ARRAY(
                    sqlalchemy_bigquery.STRUCT(
                        name=sqlalchemy.String, bdate=sqlalchemy.DATE
                    )
                ),
            ),
        ),
    )
    metadata.create_all(engine)

    conn.execute(
        table.insert().values(
            person=dict(
                name="bob",
                children=[dict(name="billy", bdate=datetime.date(2020, 1, 1))],
            )
        )
    )

    assert list(conn.execute(sqlalchemy.select([table]))) == [
        (
            {
                "name": "bob",
                "children": [{"name": "billy", "bdate": datetime.date(2020, 1, 1)}],
            },
        )
    ]
    assert list(conn.execute(sqlalchemy.select([table.c.person.NAME]))) == [("bob",)]
    assert list(conn.execute(sqlalchemy.select([table.c.person.children[0]]))) == [
        ({"name": "billy", "bdate": datetime.date(2020, 1, 1)},)
    ]
    assert list(
        conn.execute(sqlalchemy.select([table.c.person.children[0].bdate]))
    ) == [(datetime.date(2020, 1, 1),)]
    assert list(
        conn.execute(
            sqlalchemy.select([table]).where(table.c.person.children[0].NAME == "billy")
        )
    ) == [
        (
            {
                "name": "bob",
                "children": [{"name": "billy", "bdate": datetime.date(2020, 1, 1)}],
            },
        )
    ]
    assert (
        list(
            conn.execute(
                sqlalchemy.select([table]).where(
                    table.c.person.children[0].NAME == "sally"
                )
            )
        )
        == []
    )
