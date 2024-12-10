import json
from unittest import mock

import pytest
import sqlalchemy


@pytest.fixture
def json_table(metadata):
    from sqlalchemy_bigquery import JSON

    return sqlalchemy.Table("json_table", metadata, sqlalchemy.Column("json", JSON))


@pytest.fixture
def json_data():
    return {"foo": "bar"}


def test_set_json_serde(faux_conn, metadata, json_table, json_data):
    from sqlalchemy_bigquery import JSON

    json_serializer = mock.Mock(side_effect=json.dumps)
    json_deserializer = mock.Mock(side_effect=json.loads)

    engine = sqlalchemy.create_engine(
        "bigquery://myproject/mydataset",
        json_serializer=json_serializer,
        json_deserializer=json_deserializer,
    )

    json_column = json_table.c.json

    process_bind = json_column.type.bind_processor(engine.dialect)
    process_bind(json_data)
    assert json_serializer.mock_calls == [mock.call(json_data)]

    process_result = json_column.type.result_processor(engine.dialect, JSON)
    process_result(json.dumps(json_data))
    assert json_deserializer.mock_calls == [mock.call(json.dumps(json_data))]


def test_json_create(faux_conn, metadata, json_table, json_data):
    expr = sqlalchemy.schema.CreateTable(json_table)
    sql = expr.compile(faux_conn.engine).string
    assert sql == ("\nCREATE TABLE `json_table` (\n" "\t`json` JSON\n" ") \n\n")


def test_json_insert(faux_conn, metadata, json_table, json_data):
    expr = sqlalchemy.insert(json_table).values(json=json_data)
    sql = expr.compile(faux_conn.engine).string
    assert (
        sql == "INSERT INTO `json_table` (`json`) VALUES (PARSE_JSON(%(json:STRING)s))"
    )


def test_json_where(faux_conn, metadata, json_table, json_data):
    expr = sqlalchemy.select(json_table.c.json).where(json_table.c.json == json_data)
    sql = expr.compile(faux_conn.engine).string
    assert sql == (
        "SELECT `json_table`.`json` \n"
        "FROM `json_table` \n"
        "WHERE `json_table`.`json` = PARSE_JSON(%(json_1:STRING)s)"
    )
