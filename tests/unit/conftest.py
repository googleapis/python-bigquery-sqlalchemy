import contextlib
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
        # We want to bypass client creation. We don't need it and it requires creds.
        with mock.patch("pybigquery._helpers.create_bigquery_client",
                        fauxdbi.FauxClient):
            with mock.patch("google.auth.default",
                            return_value=("authdb", "authproj")):
                engine = sqlalchemy.create_engine("bigquery://myproject/mydataset")
                conn = engine.connect()
                conn.test_data = test_data

                def ex(sql, *args, **kw):
                    with contextlib.closing(conn.connection.connection.connection.cursor()
                                            ) as cursor:
                        cursor.execute(sql, *args, **kw)

                conn.ex = ex

                ex("create table comments"
                   " (key string primary key, comment string)")

                yield conn
                conn.close()

