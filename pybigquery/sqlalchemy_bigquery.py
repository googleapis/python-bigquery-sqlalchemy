"""Integration between SQLAlchemy and BigQuery."""

from __future__ import absolute_import
from __future__ import unicode_literals

from google.cloud import bigquery
from google.cloud.bigquery import dbapi, QueryJobConfig
from google.cloud.bigquery.schema import SchemaField
from google.cloud.bigquery.table import EncryptionConfiguration
from google.cloud.bigquery.dataset import DatasetReference
from google.oauth2 import service_account
from google.api_core.exceptions import NotFound
from sqlalchemy.exc import NoSuchTableError
from sqlalchemy import types, util
from sqlalchemy.sql.compiler import SQLCompiler, IdentifierPreparer
from sqlalchemy.engine.default import DefaultDialect, DefaultExecutionContext
from sqlalchemy.engine.base import Engine
from sqlalchemy.sql.schema import Column
from sqlalchemy.sql import elements
import re

from .parse_url import parse_url

FIELD_ILLEGAL_CHARACTERS = re.compile(r'[^\w]+')


class UniversalSet(object):
    """
    Set containing everything
    https://github.com/dropbox/PyHive/blob/master/pyhive/common.py
    """

    def __contains__(self, item):
        return True


class BigQueryIdentifierPreparer(IdentifierPreparer):
    """
    Set containing everything
    https://github.com/dropbox/PyHive/blob/master/pyhive/sqlalchemy_presto.py
    """
    reserved_words = UniversalSet()

    def __init__(self, dialect):
        super(BigQueryIdentifierPreparer, self).__init__(
            dialect,
            initial_quote="`",
        )

    def quote_column(self, value):
        """
        Quote a column.
        Fields are quoted separately from the record name.
        """

        parts = value.split('.')
        return '.'.join(self.quote_identifier(x) for x in parts)

    def quote(self, ident, force=None, column=False):
        """
        Conditionally quote an identifier.
        """

        force = getattr(ident, "quote", None)

        if force is None:
            if ident in self._strings:
                return self._strings[ident]
            else:
                if self._requires_quotes(ident):
                    self._strings[ident] = self.quote_column(ident) if column else self.quote_identifier(ident)
                else:
                    self._strings[ident] = ident
                return self._strings[ident]
        elif force:
            return self.quote_column(ident) if column else self.quote_identifier(ident)
        else:
            return ident

    def format_label(self, label, name=None):
        name = name or label.name

        # Fields must start with a letter or underscore
        if not name[0].isalpha() and name[0] != '_':
            name = "_" + name

        # Fields must contain only letters, numbers, and underscores
        name = FIELD_ILLEGAL_CHARACTERS.sub('_', name)

        result = self.quote(name)
        return result

_type_map = {
    'STRING': types.String,
    'BOOLEAN': types.Boolean,
    'INTEGER': types.Integer,
    'FLOAT': types.Float,
    'TIMESTAMP': types.TIMESTAMP,
    'DATETIME': types.DATETIME,
    'DATE': types.DATE,
    'BYTES': types.BINARY,
    'TIME': types.TIME,
    'RECORD': types.JSON,
    'NUMERIC': types.DECIMAL,
}


class BigQueryExecutionContext(DefaultExecutionContext):
    def create_cursor(self):
        # Set arraysize
        c = super(BigQueryExecutionContext, self).create_cursor()
        if self.dialect.arraysize:
            c.arraysize = self.dialect.arraysize
        return c


class BigQueryCompiler(SQLCompiler):
    def __init__(self, dialect, statement, column_keys=None,
                 inline=False, **kwargs):
        if isinstance(statement, Column):
            kwargs['compile_kwargs'] = util.immutabledict({'include_table': False})
        super(BigQueryCompiler, self).__init__(dialect, statement, column_keys, inline, **kwargs)

    def visit_select(self, *args, **kwargs):
        """
        Use labels for every column.
        This ensures that fields won't contain duplicate names
        """

        args[0].use_labels = True
        return super(BigQueryCompiler, self).visit_select(*args, **kwargs)

    def visit_column(self, column, add_to_result_map=None,
                     include_table=True, **kwargs):

        name = orig_name = column.name
        if name is None:
            name = self._fallback_column_name(column)

        is_literal = column.is_literal
        if not is_literal and isinstance(name, elements._truncated_label):
            name = self._truncated_identifier("colident", name)

        if add_to_result_map is not None:
            add_to_result_map(
                name,
                orig_name,
                (column, name, column.key),
                column.type
            )

        if is_literal:
            name = self.escape_literal_column(name)
        else:
            name = self.preparer.quote(name, column=True)
        table = column.table
        if table is None or not include_table or not table.named_with_column:
            return name
        else:
            effective_schema = self.preparer.schema_for_object(table)

            if effective_schema:
                schema_prefix = self.preparer.quote_schema(
                    effective_schema) + '.'
            else:
                schema_prefix = ''
            tablename = table.name
            if isinstance(tablename, elements._truncated_label):
                tablename = self._truncated_identifier("alias", tablename)
            return schema_prefix + \
                self.preparer.quote(tablename) + \
                "." + name

    def visit_label(self, *args, **kwargs):
        # Use labels in GROUP BY clause
        if len(kwargs) == 0 or len(kwargs) == 1:
            kwargs['render_label_as_label'] = args[0]
        result = super(BigQueryCompiler, self).visit_label(*args, **kwargs)
        return result


class BigQueryDialect(DefaultDialect):
    name = 'bigquery'
    driver = 'bigquery'
    preparer = BigQueryIdentifierPreparer
    statement_compiler = BigQueryCompiler
    execution_ctx_cls = BigQueryExecutionContext
    supports_alter = False
    supports_pk_autoincrement = False
    supports_default_values = False
    supports_empty_insert = False
    supports_unicode_statements = True
    supports_unicode_binds = True
    returns_unicode_strings = True
    description_encoding = None
    supports_native_boolean = True
    supports_simple_order_by_label = True
    postfetch_lastrowid = False

    def __init__(
            self,
            arraysize=5000,
            credentials_path=None,
            location=None,
            credentials_info=None,
            with_subject=None,
            *args, **kwargs):
        super(BigQueryDialect, self).__init__(*args, **kwargs)
        self.arraysize = arraysize
        self.credentials_path = credentials_path
        self.credentials_info = credentials_info
        self.with_subject = with_subject
        self.location = location
        self.dataset_id = None

    @classmethod
    def dbapi(cls):
        return dbapi

    def create_connect_args(self, url):
        location, dataset_id, arraysize, credentials_path, default_query_job_config = parse_url(url)

        self.arraysize = self.arraysize or arraysize
        self.location = location or self.location
        self.credentials_path = credentials_path or self.credentials_path
        self.dataset_id = dataset_id

        creds = None
        project = url.host

        if self.credentials_path:
            creds = service_account.Credentials.from_service_account_file(
                self.credentials_path)
        elif self.credentials_info:
            creds = service_account.Credentials.from_service_account_info(
                self.credentials_info)
            project = self.credentials_info.get('project_id')

        if self.with_subject:
            creds = creds.with_subject(self.with_subject)

        client = bigquery.Client(
            credentials=creds,
            location=self.location,
            project=project,
            default_query_job_config=default_query_job_config
        )

        # if dataset_id is set, then we know the job_config isn't None
        if dataset_id:
            default_query_job_config.default_dataset = client.dataset(dataset_id)

        return ([client], {})

    def _json_deserializer(self, row):
        """JSON deserializer for RECORD types.

        The DB-API layer already deserializes JSON to a dictionary, so this
        just returns the input.
        """
        return row

    def _split_table_name(self, full_table_name):
        # Split full_table_name to get project, dataset and table name
        dataset = None
        table_name = None
        project = None

        table_name_split = full_table_name.split('.')
        if len(table_name_split) == 2:
            dataset, table_name = table_name_split
        elif len(table_name_split) == 3:
            project, dataset, table_name = table_name_split

        return (project, dataset, table_name)

    def _get_table(self, connection, table_name, schema=None):
        if isinstance(connection, Engine):
            connection = connection.connect()

        table_name_prepared = dataset = project = None
        if self.dataset_id:
            table_name_prepared = table_name
            dataset = self.dataset_id
            project = None

        else:
            project, dataset, table_name_prepared = self._split_table_name(table_name)
            if dataset is None and schema is not None:
                table_name_prepared = table_name
                dataset = schema

        table = connection.connection._client.dataset(dataset, project=project).table(table_name_prepared)
        try:
            t = connection.connection._client.get_table(table)
        except NotFound as e:
            raise NoSuchTableError(table_name)
        return t

    def has_table(self, connection, table_name, schema=None):
        try:
            self._get_table(connection, table_name, schema)
            return True
        except NoSuchTableError:
            return False

    def _get_columns_helper(self, columns, cur_columns):
        """
        Recurse into record type and return all the nested field names.
        As contributed by @sumedhsakdeo on issue #17
        """
        results = []
        for col in columns:
            if col.field_type == 'RECORD' and col.mode != 'REPEATED':
                cur_columns.append(col)
                results += self._get_columns_helper(col.fields, cur_columns)
                cur_columns.pop()
            else:
                results += [SchemaField(name='.'.join(col.name for col in cur_columns + [col]),
                                        field_type=col.field_type,
                                        mode=col.mode,
                                        description=col.description,
                                        fields=col.fields)]
        return results

    def get_columns(self, connection, table_name, schema=None, **kw):
        table = self._get_table(connection, table_name, schema)
        columns = self._get_columns_helper(table.schema, [])
        result = []
        for col in columns:
            try:
                coltype = _type_map[col.field_type]
            except KeyError:
                util.warn("Did not recognize type '%s' of column '%s'" % (col.field_type, col.name))

            result.append({
                'name': col.name,
                'type': types.ARRAY(coltype) if col.mode == 'REPEATED' else coltype,
                'nullable': col.mode == 'NULLABLE' or col.mode == 'REPEATED',
                'default': None,
            })

        return result

    def get_foreign_keys(self, connection, table_name, schema=None, **kw):
        # BigQuery has no support for foreign keys.
        return []

    def get_pk_constraint(self, connection, table_name, schema=None, **kw):
        # BigQuery has no support for primary keys.
        return []

    def get_indexes(self, connection, table_name, schema=None, **kw):
        # BigQuery has no support for indexes.
        return []

    def get_schema_names(self, connection, **kw):
        if isinstance(connection, Engine):
            connection = connection.connect()

        datasets = connection.connection._client.list_datasets()
        if self.dataset_id is not None:
            return [d.dataset_id for d in datasets if d.dataset_id == self.dataset_id]
        else:
            return [d.dataset_id for d in datasets]

    def get_table_names(self, connection, schema=None, **kw):
        if isinstance(connection, Engine):
            connection = connection.connect()

        datasets = connection.connection._client.list_datasets()
        result = []
        for d in datasets:
            if schema is not None and d.dataset_id != schema:
                continue

            if self.dataset_id is not None and d.dataset_id != self.dataset_id:
                continue

            tables = connection.connection._client.list_tables(d.reference)
            for t in tables:
                if self.dataset_id is None:
                    table_name = d.dataset_id + '.' + t.table_id
                else:
                    table_name = t.table_id
                result.append(table_name)
        return result

    def do_rollback(self, dbapi_connection):
        # BigQuery has no support for transactions.
        pass

    def _check_unicode_returns(self, connection, additional_tests=None):
        # requests gives back Unicode strings
        return True

    def _check_unicode_description(self, connection):
        # requests gives back Unicode strings
        return True
