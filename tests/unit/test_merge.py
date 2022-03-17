import sqlalchemy as sa
import re
from sqlalchemy_bigquery import BigQueryDialect
from sqlalchemy_bigquery.merge import Merge
import pytest


def test_merge():
    into = sa.table("dest", sa.Column("b", sa.TEXT)).alias("a")
    using = (
        sa.select((sa.column("a") * 2).label("b"))
        .select_from(sa.table("world"))
        .alias("b")
    )

    merge_sql = Merge(
        into=into,
        using=using,
        on=into.c.b == using.c.b,
        when_matched_and=using.c.b > 10,
        when_matched=Merge.ThenUpdate({"b": into.c.b * using.c.b * sa.literal(123)}),
        when_not_matched_by_source=Merge.ThenInsert({"b": using.c.b}),
        when_not_matched=Merge.ThenDelete(),
    )
    merge_sql_compiled = merge_sql.compile(
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
            AND (
                `b`.`b` > 10
            )
            THEN
            UPDATE SET
                b = `a`.`b` * `b`.`b` * 123
        WHEN NOT MATCHED
            THEN
            DELETE
        WHEN NOT MATCHED BY SOURCE
            THEN
            INSERT (
                b
            ) VALUES (
                `b`.`b`
            );
    """
    assert remove_ws(merge_sql_compiled) == remove_ws(expected_sql)


def test_maybe_confusing_api():
    into = sa.table("dest", sa.Column("b", sa.TEXT)).alias("a")
    using = (
        sa.select((sa.column("a") * 2).label("b"))
        .select_from(sa.table("world"))
        .alias("b")
    )

    with pytest.raises(TypeError):
        # Maybe we can help the developer prevent this gotchya?
        str(
            Merge(
                into=into,
                using=using,
                on=into.c.b == using.c.b,
                when_not_matched=Merge.ThenDelete,
            )
        )

    str(
        Merge(
            into=into,
            using=using,
            on=into.c.b == using.c.b,
            # The dev needs to put `ThenDelete()` not `ThenDelete`
            when_not_matched=Merge.ThenDelete(),
        )
    )


def remove_ws(text: str) -> str:
    return re.sub(r"\s+", " ", str(text)).strip()
