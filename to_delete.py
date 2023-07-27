from sqlalchemy import literal, select
from sqlalchemy_bigquery.base import BigQueryDialect
query = select(literal(1).label("a:b"))

print(query.compile(compile_kwargs={"literal_binds": True}, dialect=BigQueryDialect()))

