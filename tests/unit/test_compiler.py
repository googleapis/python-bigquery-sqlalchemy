import pytest
import sqlalchemy.exc

from conftest import setup_table


def test_constraints_are_ignored(faux_conn, metadata):
    sqlalchemy.Table(
        "ref", metadata, sqlalchemy.Column("id", sqlalchemy.Integer),
    )
    sqlalchemy.Table(
        "some_table",
        metadata,
        sqlalchemy.Column("id", sqlalchemy.Integer, primary_key=True),
        sqlalchemy.Column(
            "ref_id", sqlalchemy.Integer, sqlalchemy.ForeignKey("ref.id")
        ),
        sqlalchemy.UniqueConstraint("id", "ref_id", name="uix_1"),
    )
    metadata.create_all(faux_conn.engine)
    assert " ".join(faux_conn.test_data["execute"][-1][0].strip().split()) == (
        "CREATE TABLE `some_table`" " ( `id` INT64 NOT NULL, `ref_id` INT64 )"
    )


def test_compile_column(faux_conn):
    table = setup_table(faux_conn, "t", sqlalchemy.Column("c", sqlalchemy.Integer))
    assert table.c.c.compile(faux_conn).string == "`c`"


def test_cant_compile_unnamed_column(faux_conn, metadata):
    with pytest.raises(
        sqlalchemy.exc.CompileError,
        match="Cannot compile Column object until its 'name' is assigned.",
    ):
        sqlalchemy.Column(sqlalchemy.Integer).compile(faux_conn)
