from sqlalchemy.ext.compiler import compiles
from sqlalchemy.sql.compiler import DDLCompiler

try:
    import alembic  # noqa
except ImportError:
    pass
else:
    from alembic.ddl import impl
    from alembic.ddl.base import ColumnType, format_type, alter_table, alter_column

    class SqlalchemyBigqueryImpl(impl.DefaultImpl):
        __dialect__ = "bigquery"

    @compiles(ColumnType, "bigquery")
    def visit_column_type(element: ColumnType, compiler: DDLCompiler, **kw) -> str:
        return "%s %s %s" % (
            alter_table(compiler, element.table_name, element.schema),
            alter_column(compiler, element.column_name),
            "SET DATA TYPE %s" % format_type(compiler, element.type_),
        )
