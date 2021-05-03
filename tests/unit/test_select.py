import datetime
from decimal import Decimal

import pytest
import sqlalchemy

import pybigquery.sqlalchemy_bigquery


def test_labels_not_forced(faux_conn):
    metadata = sqlalchemy.MetaData()
    table = sqlalchemy.Table(
        "some_table", metadata, sqlalchemy.Column("id", sqlalchemy.Integer)
    )
    metadata.create_all(faux_conn.engine)
    result = faux_conn.execute(sqlalchemy.select([table.c.id]))
    assert result.keys() == ["id"]  # Look! Just the column name!


def dtrepr(v):
    return f"{v.__class__.__name__.upper()} {repr(str(v))}"


@pytest.mark.parametrize(
    "type_,val,btype,vrep",
    [
        (sqlalchemy.String, "myString", "STRING", repr),
        (sqlalchemy.Text, "myText", "STRING", repr),
        (sqlalchemy.Unicode, "myUnicode", "STRING", repr),
        (sqlalchemy.UnicodeText, "myUnicodeText", "STRING", repr),
        (sqlalchemy.Integer, 424242, "INT64", repr),
        (sqlalchemy.SmallInteger, 42, "INT64", repr),
        (sqlalchemy.BigInteger, 1 << 60, "INT64", repr),
        (sqlalchemy.Numeric, Decimal(42), "NUMERIC", str),
        (sqlalchemy.Float, 4.2, "FLOAT64", repr),
        (
            sqlalchemy.DateTime,
            datetime.datetime(2021, 2, 3, 4, 5, 6, 123456),
            "DATETIME",
            dtrepr,
        ),
        (sqlalchemy.Date, datetime.date(2021, 2, 3), "DATE", dtrepr),
        (sqlalchemy.Time, datetime.time(4, 5, 6, 123456), "TIME", dtrepr),
        (sqlalchemy.Boolean, True, "BOOL", "true"),
        (sqlalchemy.REAL, 1.42, "FLOAT64", repr),
        (sqlalchemy.FLOAT, 0.42, "FLOAT64", repr),
        (sqlalchemy.NUMERIC, Decimal(4.25), "NUMERIC", str),
        (sqlalchemy.NUMERIC(39), Decimal(4.25), "BIGNUMERIC", str),
        (sqlalchemy.NUMERIC(30, 10), Decimal(4.25), "BIGNUMERIC", str),
        (sqlalchemy.NUMERIC(39, 10), Decimal(4.25), "BIGNUMERIC", str),
        (sqlalchemy.DECIMAL, Decimal(0.25), "NUMERIC", str),
        (sqlalchemy.DECIMAL(39), Decimal(4.25), "BIGNUMERIC", str),
        (sqlalchemy.DECIMAL(30, 10), Decimal(4.25), "BIGNUMERIC", str),
        (sqlalchemy.DECIMAL(39, 10), Decimal(4.25), "BIGNUMERIC", str),
        (sqlalchemy.INTEGER, 434343, "INT64", repr),
        (sqlalchemy.INT, 444444, "INT64", repr),
        (sqlalchemy.SMALLINT, 43, "INT64", repr),
        (sqlalchemy.BIGINT, 1 << 61, "INT64", repr),
        (
            sqlalchemy.TIMESTAMP,
            datetime.datetime(2021, 2, 3, 4, 5, 7, 123456),
            "TIMESTAMP",
            lambda v: f"TIMESTAMP {repr(str(v))}",
        ),
        (
            sqlalchemy.DATETIME,
            datetime.datetime(2021, 2, 3, 4, 5, 8, 123456),
            "DATETIME",
            dtrepr,
        ),
        (sqlalchemy.DATE, datetime.date(2021, 2, 4), "DATE", dtrepr),
        (sqlalchemy.TIME, datetime.time(4, 5, 7, 123456), "TIME", dtrepr),
        (sqlalchemy.TEXT, "myTEXT", "STRING", repr),
        (sqlalchemy.VARCHAR, "myVARCHAR", "STRING", repr),
        (sqlalchemy.NVARCHAR, "myNVARCHAR", "STRING", repr),
        (sqlalchemy.CHAR, "myCHAR", "STRING", repr),
        (sqlalchemy.NCHAR, "myNCHAR", "STRING", repr),
        (sqlalchemy.BINARY, b"myBINARY", "BYTES", repr),
        (sqlalchemy.VARBINARY, b"myVARBINARY", "BYTES", repr),
        (sqlalchemy.BOOLEAN, False, "BOOL", "false"),
        (sqlalchemy.ARRAY(sqlalchemy.Integer), [1, 2, 3], "ARRAY<INT64>", repr),
        (
            sqlalchemy.ARRAY(sqlalchemy.DATETIME),
            [
                datetime.datetime(2021, 2, 3, 4, 5, 6),
                datetime.datetime(2021, 2, 3, 4, 5, 7, 123456),
                datetime.datetime(2021, 2, 3, 4, 5, 8, 123456),
            ],
            "ARRAY<DATETIME>",
            lambda a: "[" + ", ".join(dtrepr(v) for v in a) + "]",
        ),
    ],
)
def test_typed_parameters(faux_conn, type_, val, btype, vrep):
    metadata = sqlalchemy.MetaData()
    col_name = "foo"
    table = sqlalchemy.Table("some_table", metadata, sqlalchemy.Column(col_name, type_))
    metadata.create_all(faux_conn.engine)

    assert faux_conn.test_data["execute"].pop()[0].strip() == (
        f"CREATE TABLE `some_table` (\n" f"\t`{col_name}` {btype}\n" f")"
    )

    faux_conn.execute(table.insert().values(**{col_name: val}))

    if btype.startswith("ARRAY<"):
        btype = btype[6:-1]

    assert faux_conn.test_data["execute"][-1] == (
        f"INSERT INTO `some_table` (`{col_name}`) VALUES (%({col_name}:{btype})s)",
        {col_name: val},
    )

    faux_conn.execute(
        table.insert()
        .values(**{col_name: sqlalchemy.literal(val, type_)})
        .compile(
            dialect=pybigquery.sqlalchemy_bigquery.BigQueryDialect(),
            compile_kwargs=dict(literal_binds=True),
        )
    )

    if not isinstance(vrep, str):
        vrep = vrep(val)

    assert faux_conn.test_data["execute"][-1] == (
        f"INSERT INTO `some_table` (`{col_name}`) VALUES ({vrep})", {})

    assert list(map(list, faux_conn.execute(sqlalchemy.select([table])))) == [[val]] * 2
    assert faux_conn.test_data["execute"][-1][0] == 'SELECT `some_table`.`foo` \nFROM `some_table`'

    assert list(map(list, faux_conn.execute(sqlalchemy.select([table.c.foo])))) == [[val]] * 2
    assert faux_conn.test_data["execute"][-1][0] == 'SELECT `some_table`.`foo` \nFROM `some_table`'


def test_select_json(faux_conn):
    metadata = sqlalchemy.MetaData()
    table = sqlalchemy.Table("t", metadata, sqlalchemy.Column("x", sqlalchemy.JSON))

    faux_conn.ex("create table t (x RECORD)")
    faux_conn.ex("""insert into t values ('{"y": 1}')""")

    row = list(faux_conn.execute(sqlalchemy.select([table])))[0]
    # We expect the raw string, because sqlite3, unlike BigQuery
    # doesn't deserialize for us.
    assert row.x == '{"y": 1}'
