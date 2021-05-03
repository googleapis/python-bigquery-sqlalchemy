def test_view_definition(faux_conn):
    cursor = faux_conn.connection.cursor()
    cursor.execute("create view foo as select 1")

    # pass the connection:
    assert faux_conn.dialect.get_view_definition(faux_conn, "foo") == "select 1"

    # pass the engine:
    assert faux_conn.dialect.get_view_definition(faux_conn.engine, "foo") == "select 1"

    # remove dataset id from dialect:
    faux_conn.dialect.dataset_id = None
    assert (
        faux_conn.dialect.get_view_definition(faux_conn, "mydataset.foo") == "select 1"
    )
