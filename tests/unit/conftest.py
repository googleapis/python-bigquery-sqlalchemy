import mock
import os
import pytest
import shutil
import sqlalchemy
import tempfile

import fauxdbi


@pytest.fixture()
def use_temporary_directory():
    here = os.getcwd()
    tdir = tempfile.mkdtemp("bq_sa_test")
    os.chdir(tdir)
    yield
    os.chdir(here)
    shutil.rmtree(tdir)


@pytest.fixture()
def faux_conn(use_temporary_directory):
    test_data = dict(execute=[])

    def factory(*args, **kw):
        conn = fauxdbi.Connection(*args, **kw)
        conn.test_data = test_data
        return conn

    with mock.patch("google.cloud.bigquery.dbapi.connection.Connection", factory):
        engine = sqlalchemy.create_engine("bigquery://myproject/mydataset")
        conn = engine.connect()
        conn.test_data = test_data
        yield conn
        conn.close()
