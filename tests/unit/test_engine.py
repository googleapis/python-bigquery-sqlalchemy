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
