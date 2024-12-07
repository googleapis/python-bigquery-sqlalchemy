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


def test_select_json(faux_conn, json_table, json_data):
    faux_conn.ex(f"create table json_table (cart JSON)")
    faux_conn.ex(f"insert into json_table values ('{json.dumps(json_data)}')")

    row = list(faux_conn.execute(sqlalchemy.select(json_table)))[0]
    assert row.cart == json_data


def test_insert_json(faux_conn, metadata, json_table, json_data):
    actual = str(
        json_table.insert()
        .values(
            cart=json_data
        )
        .compile(faux_conn.engine)
    )

    assert actual == "INSERT INTO `json_table` (`cart`) VALUES (%(cart:JSON)s)"


@pytest.mark.parametrize(
    "path,sql,literal_sql",
    (
        (
            ["name"],
            "JSON_QUERY(`json_table`.`cart`, %(cart_1:STRING)s)",
            "JSON_QUERY(`json_table`.`cart`, '$.\"name\"')",
        ),
        (
            ["items", 0],
            "JSON_QUERY(`json_table`.`cart`, %(cart_1:STRING)s)",
            "JSON_QUERY(`json_table`.`cart`, '$.\"items\"[0]')",
        ),
        (
            ["items", 0, "price"],
            "JSON_QUERY(`json_table`.`cart`, %(cart_1:STRING)s)",
            "JSON_QUERY(`json_table`.`cart`, '$.\"items\"[0].\"price\"')",
        ),
    ),
)
def test_json_query(faux_conn, json_column, path, sql, literal_sql):
    expr = sqlalchemy.select(json_column[path])

    expected_sql = f"SELECT {sql} AS `anon_1` \nFROM `json_table`"
    expected_literal_sql = f"SELECT {literal_sql} AS `anon_1` \nFROM `json_table`"

    actual_sql = expr.compile(faux_conn).string
    actual_literal_sql = expr.compile(faux_conn, compile_kwargs={"literal_binds": True}).string

    assert expected_sql == actual_sql
    assert expected_literal_sql == actual_literal_sql


def test_json_value(faux_conn, json_column, json_data):
    expr = sqlalchemy.select(json_column[["items", 0]].label("first_item")).where(sqlalchemy.func.JSON_VALUE(json_column[["name"]]) == 'Alice')

    expected_sql = f"SELECT JSON_QUERY(`json_table`.`cart`, %(cart_1:STRING)s) AS `first_item` \nFROM `json_table` \nWHERE JSON_VALUE(JSON_QUERY(`json_table`.`cart`, %(cart_2:STRING)s)) = %(JSON_VALUE_1:STRING)s"
    expected_literal_sql = f"SELECT JSON_QUERY(`json_table`.`cart`, '$.\"items\"[0]') AS `first_item` \nFROM `json_table` \nWHERE JSON_VALUE(JSON_QUERY(`json_table`.`cart`, '$.\"name\"')) = 'Alice'"

    actual_sql = expr.compile(faux_conn).string
    actual_literal_sql = expr.compile(faux_conn, compile_kwargs={"literal_binds": True}).string

    assert expected_sql == actual_sql
    assert expected_literal_sql == actual_literal_sql

# TODO: AFAICT, JSON is not a supported query parameter type - enforce this

# TODO: Test _json_serializer set from create_engine

# TODO: Casting as described in https://docs.sqlalchemy.org/en/20/core/type_basics.html#sqlalchemy.types.JSON