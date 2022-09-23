# Copyright 2021 Google LLC
#
# Use of this source code is governed by an MIT-style
# license that can be found in the LICENSE file or at
# https://opensource.org/licenses/MIT.

import re

import pytest
import sqlalchemy as sa

from sqlalchemy_bigquery import BigQueryDialect
from sqlalchemy_bigquery.merge import Merge


def test_merge():
    target = sa.table("dest", sa.Column("b", sa.TEXT)).alias("a")
    source = (
        sa.select((sa.column("a") * 2).label("b"))
        .select_from(sa.table("world"))
        .alias("b")
    )

    stmt = Merge(target=target, source=source, on=target.c.b == source.c.b)
    stmt = stmt.when_matched(source.c.b > 10).then_update(
        {"b": target.c.b * source.c.b * sa.literal(123)}
    )
    stmt = stmt.when_matched_not_matched_by_target().then_insert({"b": source.c.b})
    stmt = stmt.when_matched_not_matched_by_target().then_insert({"b": source.c.b})
    stmt = stmt.when_matched().then_delete()

    stmt_compiled = stmt.compile(
        dialect=BigQueryDialect(), compile_kwargs={"literal_binds": True}
    )
    expected_sql = """
        MERGE
            INTO `dest` AS `a`
            USING
                (SELECT `a` * 2 AS `b`
                FROM `world`) AS `b`
            ON `a`.`b` = `b`.`b`
        WHEN MATCHED
            AND `b`.`b` > 10
            THEN UPDATE SET
                b = `a`.`b` * `b`.`b` * 123
        WHEN NOT MATCHED BY TARGET
            THEN INSERT (
                b
            ) VALUES (
                `b`.`b`
            )
        WHEN NOT MATCHED BY TARGET
            THEN INSERT (
                b
            ) VALUES (
                `b`.`b`
            )
        WHEN MATCHED
            THEN DELETE;
    """
    assert remove_ws(stmt_compiled) == remove_ws(expected_sql)


def test_bad_parameter():
    target = sa.table("dest", sa.Column("b", sa.TEXT)).alias("a")
    source = (
        sa.select((sa.column("a") * 2).label("b"))
        .select_from(sa.table("world"))
        .alias("b")
    )

    with pytest.raises(TypeError):
        # Maybe we can help the developer prevent this gotchya?
        stmt = Merge(target=target, source=source, on=target.c.b == source.c.b)
        stmt = stmt.when_matched_not_matched_by_target().then_delete()  # type: ignore


def test_then_delete():
    target = sa.table("dest", sa.Column("b", sa.TEXT)).alias("a")
    source = (
        sa.select((sa.column("a") * 2).label("b"))
        .select_from(sa.table("world"))
        .alias("b")
    )

    stmt = Merge(target=target, source=source, on=target.c.b == source.c.b)
    stmt = stmt.when_matched_not_matched_by_source().then_delete()

    stmt_compiled = stmt.compile(
        dialect=BigQueryDialect(), compile_kwargs={"literal_binds": True}
    )
    print(stmt_compiled)
    expected_sql = """
        MERGE
            INTO `dest` AS `a`
            USING
                (SELECT `a` * 2 AS `b`
                FROM `world`) AS `b`
            ON `a`.`b` = `b`.`b`
        WHEN MATCHED NOT MATCHED BY SOURCE
            THEN DELETE;
    """
    assert remove_ws(stmt_compiled) == remove_ws(expected_sql)


def remove_ws(text: str) -> str:
    return re.sub(r"\s+", " ", str(text)).strip()
