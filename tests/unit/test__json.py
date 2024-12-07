import json
import pytest

import sqlalchemy


@pytest.fixture
def json_table(metadata):
    from sqlalchemy_bigquery import JSON
    return sqlalchemy.Table(
        "json_table",
        metadata,
        sqlalchemy.Column("cart", JSON),
    )


@pytest.fixture
def json_column(json_table):
    return json_table.c.cart


@pytest.fixture
def json_data():
    return {
        "name": "Alice",
        "items": [
            {"product": "book", "price": 10},
            {"product": "food", "price": 5}
        ]
    }


def test_roundtrip_json(faux_conn, json_table, json_data):
    faux_conn.ex(f"create table json_table (cart JSON)")
    faux_conn.ex(f"insert into json_table values ('{json.dumps(json_data)}')")

    row = list(faux_conn.execute(sqlalchemy.select(json_table)))[0]
    assert row.cart == json_data


def test_json_insert_type_info(faux_conn, metadata, json_table, json_data):
    actual = str(
        json_table.insert()
        .values(
            cart=json_data
        )
        .compile(faux_conn.engine)
    )

    assert actual == "INSERT INTO `json_table` (`cart`) VALUES (%(cart:JSON)s)"


@pytest.mark.parametrize(
    "index_values,sql,literal_sql",
    (
        (
            ["name"],
            "JSON_QUERY(`json_table`.`cart`, %(cart_1:STRING)s)",
            "JSON_QUERY(`json_table`.`cart`, '$.\"name\"')",
        ),
        # (
        #     ["items", 0],
        #     "JSON_QUERY(`json_table`.`cart`, %(cart_1:STRING)s)",
        #     "JSON_QUERY(`json_table`.`cart`, '$.items[0]')",
        # ),
    ),
)
def test_json_index(faux_conn, json_column, index_values, sql, literal_sql):
    expr = json_column

    for value in index_values:
        expr = expr[value]

    expected_sql = f"SELECT {sql} AS `anon_1` \nFROM `json_table`"
    expected_literal_sql = f"SELECT {literal_sql} AS `anon_1` \nFROM `json_table`"

    actual_sql = sqlalchemy.select(expr).compile(faux_conn).string
    actual_literal_sql = sqlalchemy.select(expr).compile(faux_conn, compile_kwargs={"literal_binds": True}).string

    assert expected_sql == actual_sql
    assert expected_literal_sql == actual_literal_sql

@pytest.mark.parametrize(
    "index_values,sql,literal_sql",
    (
        (
            ["name"],
            "JSON_QUERY(`json_table`.`cart`, %(cart_1:STRING)s)",
            "JSON_QUERY(`json_table`.`cart`, '$.\"name\"')",
        ),
        # (
        #     ["items", 0],
        #     "JSON_QUERY(`json_table`.`cart`, %(cart_1:STRING)s)",
        #     "JSON_QUERY(`json_table`.`cart`, '$.items[0]')",
        # ),
    ),
)
def test_json_path(faux_conn, json_column, index_values, sql, literal_sql):
    expr = json_column[index_values]

    expected_sql = f"SELECT {sql} AS `anon_1` \nFROM `json_table`"
    expected_literal_sql = f"SELECT {literal_sql} AS `anon_1` \nFROM `json_table`"

    actual_sql = sqlalchemy.select(expr).compile(faux_conn).string
    actual_literal_sql = sqlalchemy.select(expr).compile(faux_conn, compile_kwargs={"literal_binds": True}).string

    assert expected_sql == actual_sql
    assert expected_literal_sql == actual_literal_sql

# TODO: AFAICT, JSON is not a supported query parameter type - enforce this

# TODO: Test _json_serializer set from create_engine

# TODO: Casting as described in https://docs.sqlalchemy.org/en/20/core/type_basics.html#sqlalchemy.types.JSON