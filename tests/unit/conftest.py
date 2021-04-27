import mock
import os
import shutil
import sqlite3
import tempfile

import pytest
import sqlalchemy

import fauxdbi


@pytest.fixture()
def faux_conn():
    test_data = dict(execute=[])
    connection = sqlite3.connect(":memory:")

    def factory(*args, **kw):
        conn = fauxdbi.Connection(connection, test_data, *args, **kw)
        return conn

    with mock.patch("google.cloud.bigquery.dbapi.connection.Connection", factory):
        engine = sqlalchemy.create_engine("bigquery://myproject/mydataset")
        conn = engine.connect()
        conn.test_data = test_data
        yield conn
        conn.close()
