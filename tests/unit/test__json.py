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
        "items": [{"product": "book", "price": 10}, {"product": "food", "price": 5}],
    }


def test_select_json(faux_conn, json_table, json_data):
    faux_conn.ex("create table json_table (cart JSON)")
    faux_conn.ex(f"insert into json_table values ('{json.dumps(json_data)}')")

    row = list(faux_conn.execute(sqlalchemy.select(json_table)))[0]
    assert row.cart == json_data


def test_insert_json(faux_conn, metadata, json_table, json_data):
    actual = str(json_table.insert().values(cart=json_data).compile(faux_conn.engine))

    assert (
        actual
        == "INSERT INTO `json_table` (`cart`) VALUES (PARSE_JSON(%(cart:STRING)s))"
    )


@pytest.mark.parametrize(
    "path,literal_sql",
    (
        (
            ["name"],
            "JSON_QUERY(`json_table`.`cart`, '$.\"name\"')",
        ),
        (
            ["items", 0],
            "JSON_QUERY(`json_table`.`cart`, '$.\"items\"[0]')",
        ),
        (
            ["items", 0, "price"],
            'JSON_QUERY(`json_table`.`cart`, \'$."items"[0]."price"\')',
        ),
    ),
)
def test_json_query(faux_conn, json_column, path, literal_sql):
    expr = sqlalchemy.select(json_column[path])

    expected_sql = (
        "SELECT JSON_QUERY(`json_table`.`cart`, %(cart_1:STRING)s) AS `anon_1` \n"
        "FROM `json_table`"
    )
    expected_literal_sql = f"SELECT {literal_sql} AS `anon_1` \nFROM `json_table`"

    actual_sql = expr.compile(faux_conn).string
    actual_literal_sql = expr.compile(
        faux_conn, compile_kwargs={"literal_binds": True}
    ).string

    assert expected_sql == actual_sql
    assert expected_literal_sql == actual_literal_sql


def test_json_value(faux_conn, json_column, json_data):
    expr = sqlalchemy.select(json_column[["items", 0]].label("first_item")).where(
        sqlalchemy.func.JSON_VALUE(json_column[["name"]]) == "Alice"
    )

    expected_sql = (
        "SELECT JSON_QUERY(`json_table`.`cart`, %(cart_1:STRING)s) AS `first_item` \n"
        "FROM `json_table` \n"
        "WHERE JSON_VALUE(JSON_QUERY(`json_table`.`cart`, %(cart_2:STRING)s)) = %(JSON_VALUE_1:STRING)s"
    )
    expected_literal_sql = (
        "SELECT JSON_QUERY(`json_table`.`cart`, '$.\"items\"[0]') AS `first_item` \n"
        "FROM `json_table` \n"
        "WHERE JSON_VALUE(JSON_QUERY(`json_table`.`cart`, '$.\"name\"')) = 'Alice'"
    )

    actual_sql = expr.compile(faux_conn).string
    actual_literal_sql = expr.compile(
        faux_conn, compile_kwargs={"literal_binds": True}
    ).string

    assert expected_sql == actual_sql
    assert expected_literal_sql == actual_literal_sql


def test_json_literal(faux_conn):
    from sqlalchemy_bigquery import JSON

    expr = sqlalchemy.select(
        sqlalchemy.func.STRING(
            sqlalchemy.sql.expression.literal("purple", type_=JSON)
        ).label("color")
    )

    expected_sql = "SELECT STRING(PARSE_JSON(%(param_1:STRING)s)) AS `color`"
    expected_literal_sql = "SELECT STRING(PARSE_JSON('\"purple\"')) AS `color`"

    actual_sql = expr.compile(faux_conn).string
    actual_literal_sql = expr.compile(
        faux_conn, compile_kwargs={"literal_binds": True}
    ).string

    assert expected_sql == actual_sql
    assert expected_literal_sql == actual_literal_sql


@pytest.mark.parametrize("lax,prefix", ((False, ""), (True, "LAX_")))
def test_json_casts(faux_conn, json_column, json_data, lax, prefix):
    from sqlalchemy_bigquery import JSON

    expr = sqlalchemy.select(1).where(
        json_column[["name"]].as_string(lax=lax) == "Alice"
    )
    assert expr.compile(faux_conn, compile_kwargs={"literal_binds": True}).string == (
        "SELECT 1 \n"
        "FROM `json_table` \n"
        f"WHERE {prefix}STRING(JSON_QUERY(`json_table`.`cart`, '$.\"name\"')) = 'Alice'"
    )

    expr = sqlalchemy.select(1).where(
        json_column[["items", 1, "price"]].as_integer(lax=lax) == 10
    )
    assert expr.compile(faux_conn, compile_kwargs={"literal_binds": True}).string == (
        "SELECT 1 \n"
        "FROM `json_table` \n"
        f'WHERE {prefix}INT64(JSON_QUERY(`json_table`.`cart`, \'$."items"[1]."price"\')) = 10'
    )

    expr = sqlalchemy.select(
        sqlalchemy.literal(10.0, type_=JSON).as_float(lax=lax) == 10.0
    )
    assert expr.compile(faux_conn, compile_kwargs={"literal_binds": True}).string == (
        f"SELECT {prefix}FLOAT64(PARSE_JSON('10.0')) = 10.0 AS `anon_1`"
    )

    expr = sqlalchemy.select(
        sqlalchemy.literal(True, type_=JSON).as_boolean(lax=lax) == sqlalchemy.true()
    )
    assert expr.compile(faux_conn, compile_kwargs={"literal_binds": True}).string == (
        f"SELECT {prefix}BOOL(PARSE_JSON('true')) = true AS `anon_1`"
    )


@pytest.mark.parametrize(
    "mode,prefix", ((None, ""), ("LAX", "lax "), ("LAX_RECURSIVE", "lax recursive "))
)
def test_json_path_mode(faux_conn, json_column, mode, prefix):
    from sqlalchemy_bigquery import JSON

    if mode == "LAX":
        path = [JSON.JSONPathMode.LAX, "items", "price"]
    elif mode == "LAX_RECURSIVE":
        path = [JSON.JSONPathMode.LAX_RECURSIVE, "items", "price"]
    else:
        path = ["items", "price"]

    expr = sqlalchemy.select(json_column[path])

    expected_literal_sql = (
        f'SELECT JSON_QUERY(`json_table`.`cart`, \'{prefix}$."items"."price"\') AS `anon_1` \n'
        "FROM `json_table`"
    )
    actual_literal_sql = expr.compile(
        faux_conn, compile_kwargs={"literal_binds": True}
    ).string

    assert expected_literal_sql == actual_literal_sql
