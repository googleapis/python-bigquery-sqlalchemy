# Copyright (c) 2021 The sqlalchemy-bigquery Authors
#
# Permission is hereby granted, free of charge, to any person obtaining a copy of
# this software and associated documentation files (the "Software"), to deal in
# the Software without restriction, including without limitation the rights to
# use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
# the Software, and to permit persons to whom the Software is furnished to do so,
# subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
# FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
# COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER
# IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
# CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

import datetime
import sqlite3
import pytest
import sqlalchemy

from .conftest import setup_table


def test_table_expiration_timestamp_dialect_option(faux_conn):
    setup_table(
        faux_conn,
        "some_table",
        sqlalchemy.Column("createdAt", sqlalchemy.DateTime),
        bigquery_expiration_timestamp=datetime.datetime.fromisoformat(
            "2038-01-01T00:00:00+00:00"
        ),
    )

    assert " ".join(faux_conn.test_data["execute"][-1][0].strip().split()) == (
        "CREATE TABLE `some_table` ( `createdAt` DATETIME )"
        " OPTIONS(expiration_timestamp=TIMESTAMP '2038-01-01 00:00:00+00:00')"
    )


def test_table_partition_expiration_days_dialect_option(faux_conn):
    setup_table(
        faux_conn,
        "some_table",
        sqlalchemy.Column("createdAt", sqlalchemy.DateTime),
        bigquery_partition_expiration_days=30,
    )

    assert " ".join(faux_conn.test_data["execute"][-1][0].strip().split()) == (
        "CREATE TABLE `some_table` ( `createdAt` DATETIME )"
        " OPTIONS(partition_expiration_days=30)"
    )


def test_table_require_partition_filter_dialect_option(faux_conn):
    setup_table(
        faux_conn,
        "some_table",
        sqlalchemy.Column("createdAt", sqlalchemy.DateTime),
        bigquery_require_partition_filter=True,
    )

    assert " ".join(faux_conn.test_data["execute"][-1][0].strip().split()) == (
        "CREATE TABLE `some_table` ( `createdAt` DATETIME )"
        " OPTIONS(require_partition_filter=true)"
    )


def test_table_default_rounding_mode_dialect_option(faux_conn):
    setup_table(
        faux_conn,
        "some_table",
        sqlalchemy.Column("createdAt", sqlalchemy.DateTime),
        bigquery_default_rounding_mode="ROUND_HALF_EVEN",
    )

    assert " ".join(faux_conn.test_data["execute"][-1][0].strip().split()) == (
        "CREATE TABLE `some_table` ( `createdAt` DATETIME )"
        " OPTIONS(default_rounding_mode='ROUND_HALF_EVEN')"
    )


def test_table_clustering_fields_dialect_option_no_such_column(faux_conn):
    with pytest.raises(sqlalchemy.exc.NoSuchColumnError):
        setup_table(
            faux_conn,
            "some_table",
            sqlalchemy.Column("id", sqlalchemy.Integer),
            sqlalchemy.Column("createdAt", sqlalchemy.DateTime),
            bigquery_clustering_fields=["country", "unknown"],
        )


def test_table_clustering_fields_dialect_option(faux_conn):
    # expect table creation to fail as SQLite does not support clustering
    with pytest.raises(sqlite3.OperationalError):
        setup_table(
            faux_conn,
            "some_table",
            sqlalchemy.Column("id", sqlalchemy.Integer),
            sqlalchemy.Column("country", sqlalchemy.Text),
            sqlalchemy.Column("town", sqlalchemy.Text),
            bigquery_clustering_fields=["country", "town"],
        )

    assert " ".join(faux_conn.test_data["execute"][-1][0].strip().split()) == (
        "CREATE TABLE `some_table` ( `id` INT64, `country` STRING, `town` STRING )"
        " CLUSTER BY country, town"
    )


def test_table_clustering_fields_dialect_option_type_error(faux_conn):
    # expect TypeError when bigquery_clustering_fields is not a list
    with pytest.raises(TypeError):
        setup_table(
            faux_conn,
            "some_table",
            sqlalchemy.Column("id", sqlalchemy.Integer),
            sqlalchemy.Column("country", sqlalchemy.Text),
            sqlalchemy.Column("town", sqlalchemy.Text),
            bigquery_clustering_fields="country, town",
        )


def test_table_partitioning_dialect_option(faux_conn):
    # expect table creation to fail as SQLite does not support partitioned tables
    with pytest.raises(sqlite3.OperationalError):
        setup_table(
            faux_conn,
            "some_table",
            sqlalchemy.Column("id", sqlalchemy.Integer),
            sqlalchemy.Column("createdAt", sqlalchemy.DateTime),
            bigquery_partitioning="DATE(createdAt)",
        )

    assert " ".join(faux_conn.test_data["execute"][-1][0].strip().split()) == (
        "CREATE TABLE `some_table` ( `id` INT64, `createdAt` DATETIME )"
        " PARTITION BY DATE(createdAt)"
    )


def test_table_partitioning_dialect_option_type_error(faux_conn):
    # expect TypeError when bigquery_partitioning is not a string
    with pytest.raises(TypeError):
        setup_table(
            faux_conn,
            "some_table",
            sqlalchemy.Column("id", sqlalchemy.Integer),
            sqlalchemy.Column("createdAt", sqlalchemy.DateTime),
            bigquery_partitioning=["DATE(createdAt)"],
        )


def test_table_all_dialect_option(faux_conn):
    # expect table creation to fail as SQLite does not support clustering and partitioned tables
    with pytest.raises(sqlite3.OperationalError):
        setup_table(
            faux_conn,
            "some_table",
            sqlalchemy.Column("id", sqlalchemy.Integer),
            sqlalchemy.Column("country", sqlalchemy.Text),
            sqlalchemy.Column("town", sqlalchemy.Text),
            sqlalchemy.Column("createdAt", sqlalchemy.DateTime),
            bigquery_expiration_timestamp=datetime.datetime.fromisoformat(
                "2038-01-01T00:00:00+00:00"
            ),
            bigquery_partition_expiration_days=30,
            bigquery_require_partition_filter=True,
            bigquery_default_rounding_mode="ROUND_HALF_EVEN",
            bigquery_clustering_fields=["country", "town"],
            bigquery_partitioning="DATE(createdAt)",
        )

    assert " ".join(faux_conn.test_data["execute"][-1][0].strip().split()) == (
        "CREATE TABLE `some_table` ( `id` INT64, `country` STRING, `town` STRING, `createdAt` DATETIME )"
        " PARTITION BY DATE(createdAt)"
        " CLUSTER BY country, town"
        " OPTIONS(expiration_timestamp=TIMESTAMP '2038-01-01 00:00:00+00:00', partition_expiration_days=30, require_partition_filter=true, default_rounding_mode='ROUND_HALF_EVEN')"
    )
