==================================
SQLAlchemy Dialog Compliance Tests
==================================

SQLAlchemy provides reusable tests that test that SQLAlchemy dialects
work properly. This directory applies these tests to the BigQuery
SQLAlchemy dialect.

These are "system" tests, meaning that they run against a real
BigQuery account. To run the tests, you need a BigQuery account with
empty `test_pybigquery_sqla` and `test_schema` schemas. You need to
have the `GOOGLE_APPLICATION_CREDENTIALS` environment variable set to
the path of a Google Cloud authentication file.

Multiple simultaneous test runs
================================

The compliance test use the schemes/datasets `test_pybigquery_sqla`
and `test_schema`.  If you want to be able to run the test more than
once at the same time, for example to work on different branches or to
develop while continuous integration is running, you'll want to use
the `--dburi` option to specify a schema other than
`test_pybigquery_sqla` and the `--requirements` option to specify the
`NoSchemas` class to disable test that test support for multiple
schemas. For example::

  nox -s compliance -- \
     --dburi bigquery:///test_pybigquery_sqla2 \
     --requirements pybigquery.requirements:NoSchemas
