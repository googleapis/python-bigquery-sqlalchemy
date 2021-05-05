import sqlalchemy

from conftest import setup_table


def test_inline_comments(faux_conn):
    setup_table(
        faux_conn,
        "some_table",
        sqlalchemy.Column("id", sqlalchemy.Integer, comment="identifier"),
        comment="a fine table",
    )

    dialect = faux_conn.dialect
    assert dialect.get_table_comment(faux_conn, "some_table") == {
        "text": "a fine table"
    }
    assert dialect.get_columns(faux_conn, "some_table")[0]["comment"] == "identifier"


def test_set_drop_table_comment(faux_conn):
    table = setup_table(
        faux_conn, "some_table", sqlalchemy.Column("id", sqlalchemy.Integer),
    )

    dialect = faux_conn.dialect
    assert dialect.get_table_comment(faux_conn, "some_table") == {"text": None}

    table.comment = "a fine table"
    faux_conn.execute(sqlalchemy.schema.SetTableComment(table))
    assert dialect.get_table_comment(faux_conn, "some_table") == {
        "text": "a fine table"
    }

    faux_conn.execute(sqlalchemy.schema.DropTableComment(table))
    assert dialect.get_table_comment(faux_conn, "some_table") == {"text": None}


def test_table_description_dialect_option(faux_conn):
    setup_table(
        faux_conn,
        "some_table",
        sqlalchemy.Column("id", sqlalchemy.Integer),
        bigquery_description="a fine table",
    )
    dialect = faux_conn.dialect
    assert dialect.get_table_comment(faux_conn, "some_table") == {
        "text": "a fine table"
    }


def test_table_friendly_name_dialect_option(faux_conn):
    setup_table(
        faux_conn,
        "some_table",
        sqlalchemy.Column("id", sqlalchemy.Integer),
        bigquery_friendly_name="bob",
    )

    assert " ".join(faux_conn.test_data["execute"][-1][0].strip().split()) == (
        "CREATE TABLE `some_table` ( `id` INT64 )" " OPTIONS(friendly_name='bob')"
    )


def test_table_friendly_name_description_dialect_option(faux_conn):
    setup_table(
        faux_conn,
        "some_table",
        sqlalchemy.Column("id", sqlalchemy.Integer),
        bigquery_friendly_name="bob",
        bigquery_description="a fine table",
    )

    dialect = faux_conn.dialect
    assert dialect.get_table_comment(faux_conn, "some_table") == {
        "text": "a fine table"
    }
    assert " ".join(faux_conn.test_data["execute"][-1][0].strip().split()) == (
        "CREATE TABLE `some_table` ( `id` INT64 )"
        " OPTIONS(description='a fine table', friendly_name='bob')"
    )
