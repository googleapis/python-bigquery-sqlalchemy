import sqlalchemy_bigquery
import sqlalchemy_bigquery.version


def test_sqlalchemy_bigquery_has_version():
    assert isinstance(sqlalchemy_bigquery.__version__, str)


def test_version_module_has_version():
    assert isinstance(sqlalchemy_bigquery.version.__version__, str)
