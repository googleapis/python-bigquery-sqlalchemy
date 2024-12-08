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
import json
from unittest import mock

import pytest
import sqlalchemy


def test_engine_dataset_but_no_project(faux_conn):
    engine = sqlalchemy.create_engine("bigquery:///foo")
    conn = engine.connect()
    assert conn.connection._client.project == "authproj"


def test_engine_no_dataset_no_project(faux_conn):
    engine = sqlalchemy.create_engine("bigquery://")
    conn = engine.connect()
    assert conn.connection._client.project == "authproj"


@pytest.mark.parametrize("arraysize", [0, None])
def test_set_arraysize_not_set_if_false(faux_conn, metadata, arraysize):
    engine = sqlalchemy.create_engine("bigquery://", arraysize=arraysize)
    sqlalchemy.Table("t", metadata, sqlalchemy.Column("c", sqlalchemy.Integer))
    conn = engine.connect()
    metadata.create_all(engine)

    # Because we gave a false array size, the array size wasn't set on the cursor:
    assert "arraysize" not in conn.connection.test_data


def test_set_arraysize(faux_conn, metadata):
    engine = sqlalchemy.create_engine("bigquery://", arraysize=42)
    sqlalchemy.Table("t", metadata, sqlalchemy.Column("c", sqlalchemy.Integer))
    conn = engine.connect()
    metadata.create_all(engine)

    # Because we gave a false array size, the array size wasn't set on the cursor:
    assert conn.connection.test_data["arraysize"] == 42


def test_arraysize_querystring_takes_precedence_over_default(faux_conn, metadata):
    arraysize = 42
    engine = sqlalchemy.create_engine(
        f"bigquery://myproject/mydataset?arraysize={arraysize}"
    )
    sqlalchemy.Table("t", metadata, sqlalchemy.Column("c", sqlalchemy.Integer))
    conn = engine.connect()
    metadata.create_all(engine)

    assert conn.connection.test_data["arraysize"] == arraysize


def test_set_json_serde(faux_conn, metadata):
    from sqlalchemy_bigquery import JSON

    json_serializer = mock.Mock(side_effect=json.dumps)
    json_deserializer = mock.Mock(side_effect=json.loads)

    engine = sqlalchemy.create_engine(
        f"bigquery://myproject/mydataset",
        json_serializer=json_serializer,
        json_deserializer=json_deserializer,
    )

    json_data = {"foo": "bar"}
    json_table = sqlalchemy.Table(
        "json_table", metadata, sqlalchemy.Column("json", JSON)
    )

    metadata.create_all(engine)
    faux_conn.ex(f"insert into json_table values ('{json.dumps(json_data)}')")

    with engine.begin() as conn:
        row = conn.execute(sqlalchemy.select(json_table.c.json)).first()
        assert row == (json_data,)
        assert json_deserializer.mock_calls == [mock.call(json.dumps(json_data))]

    expr = sqlalchemy.select(sqlalchemy.literal(json_data, type_=JSON))
    literal_sql = expr.compile(engine, compile_kwargs={"literal_binds": True}).string
    assert literal_sql == f"SELECT PARSE_JSON('{json.dumps(json_data)}') AS `anon_1`"
    assert json_serializer.mock_calls == [mock.call(json_data)]
