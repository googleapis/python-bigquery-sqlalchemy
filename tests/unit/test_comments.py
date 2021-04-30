import sqlalchemy

def test_inline_comments(faux_conn):
    metadata = sqlalchemy.MetaData()
    table = sqlalchemy.Table(
        "some_table",
        metadata,
        sqlalchemy.Column("id", sqlalchemy.Integer, comment="identifier"),
        comment="a fine table",
    )
    metadata.create_all(faux_conn.engine)

    dialect = faux_conn.dialect
    assert dialect.get_table_comment(faux_conn, "some_table") == {'text': 'a fine table'}
    assert dialect.get_columns(faux_conn, "some_table")[0]['comment'] == 'identifier'

def test_set_drop_table_comment(faux_conn):

    metadata = sqlalchemy.MetaData()
    table = sqlalchemy.Table(
        "some_table",
        metadata,
        sqlalchemy.Column("id", sqlalchemy.Integer),
    )
    metadata.create_all(faux_conn.engine)

    dialect = faux_conn.dialect
    assert dialect.get_table_comment(faux_conn, "some_table") == {'text': None}

    table.comment = "a fine table"
    faux_conn.execute(sqlalchemy.schema.SetTableComment(table))
    assert dialect.get_table_comment(faux_conn, "some_table") == {'text': 'a fine table'}

    faux_conn.execute(sqlalchemy.schema.DropTableComment(table))
    assert dialect.get_table_comment(faux_conn, "some_table") == {'text': None}
