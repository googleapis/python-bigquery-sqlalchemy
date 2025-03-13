# Copyright 2021 The sqlalchemy-bigquery Authors
#
# Use of this source code is governed by an MIT-style
# license that can be found in the LICENSE file or at
# https://opensource.org/licenses/MIT.

from unittest import mock

import google.api_core.exceptions
from google.cloud import bigquery
from google.cloud.bigquery.dataset import DatasetListItem
from google.cloud.bigquery.table import TableListItem
import pytest
import sqlalchemy

from .conftest import setup_table


@pytest.fixture
def mock_bigquery_client():
    return mock.create_autospec(bigquery.Client, instance=True)


@pytest.fixture
def mock_connection(monkeypatch, mock_bigquery_client):
    import sqlalchemy_bigquery

    def mock_connect_args(*args, **kwargs):
        return ([mock_bigquery_client], {})

    monkeypatch.setattr(
        sqlalchemy_bigquery.BigQueryDialect, "create_connect_args", mock_connect_args
    )


@pytest.fixture
def engine_under_test(mock_connection):
    return sqlalchemy.create_engine("bigquery://")


@pytest.fixture
def inspector_under_test(engine_under_test):
    from sqlalchemy.engine.reflection import Inspector

    return Inspector.from_engine(engine_under_test)


def dataset_item(dataset_id):
    return DatasetListItem(
        {"datasetReference": {"projectId": "some-project-id", "datasetId": dataset_id}}
    )


def table_item(dataset_id, table_id, type_="TABLE"):
    return TableListItem(
        {
            "type": type_,
            "tableReference": {
                "projectId": "some-project-id",
                "datasetId": dataset_id,
                "tableId": table_id,
            },
        }
    )


@pytest.mark.parametrize(
    ["dataset", "tables_list", "expected"],
    [
        (None, [], []),
        ("dataset", [], []),
        (
            "dataset",
            [
                table_item("dataset", "t1"),
                table_item("dataset", "view", type_="VIEW"),
                table_item("dataset", "ext", type_="EXTERNAL"),
                table_item("dataset", "mv", type_="MATERIALIZED_VIEW"),
            ],
            ["t1", "ext"],
        ),
        (
            "dataset",
            google.api_core.exceptions.NotFound("dataset_deleted"),
            [],
        ),
    ],
)
def test_get_table_names(
    engine_under_test, mock_bigquery_client, dataset, tables_list, expected
):
    mock_bigquery_client.list_tables.side_effect = [tables_list]
    table_names = sqlalchemy.inspect(engine_under_test).get_table_names(schema=dataset)
    if dataset:
        mock_bigquery_client.list_tables.assert_called_once()
    else:
        mock_bigquery_client.list_tables.assert_not_called()
    assert list(sorted(table_names)) == list(sorted(expected))


@pytest.mark.parametrize(
    ["dataset", "tables_list", "expected"],
    [
        (None, [], []),
        ("dataset", [], []),
        (
            "dataset",
            [
                table_item("dataset", "t1"),
                table_item("dataset", "view", type_="VIEW"),
                table_item("dataset", "ext", type_="EXTERNAL"),
                table_item("dataset", "mv", type_="MATERIALIZED_VIEW"),
            ],
            ["view", "mv"],
        ),
        (
            "dataset_deleted",
            google.api_core.exceptions.NotFound("dataset_deleted"),
            [],
        ),
    ],
)
def test_get_view_names(
    inspector_under_test, mock_bigquery_client, dataset, tables_list, expected
):
    mock_bigquery_client.list_tables.side_effect = [tables_list]
    view_names = inspector_under_test.get_view_names(schema=dataset)
    if dataset:
        mock_bigquery_client.list_tables.assert_called_once()
    else:
        mock_bigquery_client.list_tables.assert_not_called()
    assert list(sorted(view_names)) == list(sorted(expected))


@pytest.mark.parametrize(
    "inp, outp",
    [
        ("(NULL IN UNNEST([ NULL) AND (1 != 1 ]))", "(NULL IN(NULL) AND (1 != 1))"),
        (
            "(NULL IN UNNEST([ NULL) AND (1 != 1:INT64 ]))",
            "(NULL IN(NULL) AND (1 != 1))",
        ),
        (
            "(NULL IN UNNEST([ (NULL, NULL)) AND (1 != 1:INT64 ]))",
            "(NULL IN((NULL, NULL)) AND (1 != 1))",
        ),
    ],
)
def test__remove_type_from_empty_in(inp, outp):
    from sqlalchemy_bigquery.base import BigQueryExecutionContext

    r = BigQueryExecutionContext._BigQueryExecutionContext__remove_type_from_empty_in
    assert r(None, inp) == outp


def test_multi_value_insert(faux_conn, last_query):
    table = setup_table(faux_conn, "t", sqlalchemy.Column("id", sqlalchemy.Integer))
    faux_conn.execute(table.insert().values([dict(id=i) for i in range(3)]))

    last_query(
        "INSERT INTO `t` (`id`) VALUES"
        " (%(id_m0:INT64)s), (%(id_m1:INT64)s), (%(id_m2:INT64)s)",
        {"id_m0": 0, "id_m1": 1, "id_m2": 2},
    )


def test_follow_dialect_attribute_convention():
    import sqlalchemy_bigquery.base

    assert sqlalchemy_bigquery.dialect is sqlalchemy_bigquery.BigQueryDialect
    assert sqlalchemy_bigquery.base.dialect is sqlalchemy_bigquery.BigQueryDialect


@pytest.mark.parametrize(
    "args,kw,error",
    [
        ((), {}, "The unnest function requires a single argument."),
        ((1, 1), {}, "The unnest function requires a single argument."),
        ((1,), {"expr": 1}, "The unnest function requires a single argument."),
        ((1, 1), {"expr": 1}, "The unnest function requires a single argument."),
        (
            (),
            {"expr": sqlalchemy.Column("x", sqlalchemy.String)},
            "The argument to unnest must have an ARRAY type.",
        ),
        (
            (sqlalchemy.Column("x", sqlalchemy.String),),
            {},
            "The argument to unnest must have an ARRAY type.",
        ),
    ],
)
def test_unnest_function_errors(args, kw, error):
    # Make sure the unnest function is registered with SQLAlchemy, which
    # happens when sqlalchemy_bigquery is imported.
    import sqlalchemy_bigquery  # noqa

    with pytest.raises(TypeError, match=error):
        sqlalchemy.func.unnest(*args, **kw)


@pytest.mark.parametrize(
    "args,kw",
    [
        ((), {"expr": sqlalchemy.Column("x", sqlalchemy.ARRAY(sqlalchemy.String))}),
        ((sqlalchemy.Column("x", sqlalchemy.ARRAY(sqlalchemy.String)),), {}),
    ],
)
def test_unnest_function(args, kw):
    # Make sure the unnest function is registered with SQLAlchemy, which
    # happens when sqlalchemy_bigquery is imported.
    import sqlalchemy_bigquery  # noqa

    f = sqlalchemy.func.unnest(*args, **kw)
    assert isinstance(f.type, sqlalchemy.String)
    assert isinstance(sqlalchemy.select(f).subquery().c.unnest.type, sqlalchemy.String)


@mock.patch("sqlalchemy_bigquery._helpers.create_bigquery_client")
def test_setting_user_supplied_client_skips_creating_client(
    mock_create_bigquery_client,
):
    import sqlalchemy_bigquery  # noqa

    result = sqlalchemy_bigquery.BigQueryDialect().create_connect_args(
        mock.MagicMock(database=None, query={"user_supplied_client": "true"})
    )
    assert result == ([], {})
    assert not mock_create_bigquery_client.called


def test_do_execute():
    # Ensures the do_execute() method overrides that of the parent class.
    import sqlalchemy_bigquery  # noqa
    from sqlalchemy_bigquery.base import BigQueryExecutionContext

    job_config_kwargs = {}
    job_config_kwargs["use_query_cache"] = False
    job_config = bigquery.QueryJobConfig(**job_config_kwargs)
    execution_options = {"job_config": job_config}
    context = mock.MagicMock(spec=BigQueryExecutionContext)
    type(context).execution_options = mock.PropertyMock(return_value=execution_options)

    cursor = mock.MagicMock()

    sqlalchemy_bigquery.BigQueryDialect().do_execute(
        cursor, sqlalchemy.text("SELECT 'a' AS `1`"), mock.MagicMock(), context=context
    )

    assert cursor.execute.call_args.kwargs["job_config"] is job_config
