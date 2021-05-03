import mock
import sqlalchemy


def test_engine_dataset_but_no_project(faux_conn):
    engine = sqlalchemy.create_engine("bigquery:///foo")
    conn = engine.connect()
    assert conn.connection._client.project == "authproj"


def test_engine_no_dataset_no_project(faux_conn):
    engine = sqlalchemy.create_engine("bigquery://")
    conn = engine.connect()
    assert conn.connection._client.project == "authproj"
