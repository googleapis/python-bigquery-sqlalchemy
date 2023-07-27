import sqlalchemy as sa
import sqlalchemy.dialects.sqlite as sqlite
import sqlalchemy_bigquery as bigquery

print("sqlalchemy:         ", sa.__version__)
print("sqlalchemy-bigquery:", bigquery.__version__)

engine = sa.create_engine("sqlite://")
metadata = sa.MetaData()

sensor_table = sa.Table(
    "sensor",
    metadata,
    sa.Column("time", sa.TIMESTAMP),
    sa.Column("value", sa.String),
)

stmt = sa.select(
    sa.func.timestamp_trunc(sensor_table.c.time, sa.text("hour")).label("time"),
    sa.func.max(sensor_table.c.value),
).group_by(
    sa.func.timestamp_trunc(sensor_table.c.time, sa.text("hour")),
)

print("SQLite" + "-" * 40)
print(
    stmt.compile(
        dialect=sqlite.dialect(),
        compile_kwargs={"literal_binds": True},
    ),
)

print("BigQuery" + "-" * 40)
print(
    stmt.compile(
        dialect=bigquery.dialect(),
        compile_kwargs={"literal_binds": True},
    ),
)