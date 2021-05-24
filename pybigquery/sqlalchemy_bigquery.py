# Copyright (c) 2017 The PyBigQuery Authors
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

"""Integration between SQLAlchemy and BigQuery."""

from __future__ import absolute_import
from __future__ import unicode_literals

from decimal import Decimal
import random
import operator
import uuid

from google import auth
import google.api_core.exceptions
from google.cloud.bigquery import dbapi
from google.cloud.bigquery.schema import SchemaField
from google.cloud.bigquery.table import TableReference
from google.api_core.exceptions import NotFound

import sqlalchemy
import sqlalchemy.sql.sqltypes
import sqlalchemy.sql.type_api
from sqlalchemy.exc import NoSuchTableError
from sqlalchemy import types, util
from sqlalchemy.sql.compiler import (
    SQLCompiler,
    GenericTypeCompiler,
    DDLCompiler,
    IdentifierPreparer,
)
from sqlalchemy.sql.sqltypes import Integer, String, NullType, Numeric
from sqlalchemy.engine.default import DefaultDialect, DefaultExecutionContext
from sqlalchemy.engine.base import Engine
from sqlalchemy.sql.schema import Column
from sqlalchemy.sql import elements, selectable
import re

from .parse_url import parse_url
from pybigquery import _helpers

FIELD_ILLEGAL_CHARACTERS = re.compile(r"[^\w]+")


def assert_(cond, message="Assertion failed"):  # pragma: NO COVER
    if not cond:
        raise AssertionError(message)


class BigQueryIdentifierPreparer(IdentifierPreparer):
    """
    Set containing everything
    https://github.com/dropbox/PyHive/blob/master/pyhive/sqlalchemy_presto.py
    """

    def __init__(self, dialect):
        super(BigQueryIdentifierPreparer, self).__init__(
            dialect, initial_quote="`",
        )

    def quote_column(self, value):
        """
        Quote a column.
        Fields are quoted separately from the record name.
        """

        parts = value.split(".")
        return ".".join(self.quote_identifier(x) for x in parts)

    def quote(self, ident, force=None, column=False):
        """
        Conditionally quote an identifier.
        """

        force = getattr(ident, "quote", None)
        if force is None or force:
            return self.quote_column(ident) if column else self.quote_identifier(ident)
        else:
            return ident

    def format_label(self, label, name=None):
        name = name or label.name

        # Fields must start with a letter or underscore
        if not name[0].isalpha() and name[0] != "_":
            name = "_" + name

        # Fields must contain only letters, numbers, and underscores
        name = FIELD_ILLEGAL_CHARACTERS.sub("_", name)

        result = self.quote(name)
        return result


_type_map = {
    "STRING": types.String,
    "BOOL": types.Boolean,
    "BOOLEAN": types.Boolean,
    "INT64": types.Integer,
    "INTEGER": types.Integer,
    "FLOAT64": types.Float,
    "FLOAT": types.Float,
    "TIMESTAMP": types.TIMESTAMP,
    "DATETIME": types.DATETIME,
    "DATE": types.DATE,
    "BYTES": types.BINARY,
    "TIME": types.TIME,
    "RECORD": types.JSON,
    "NUMERIC": types.Numeric,
    "BIGNUMERIC": types.Numeric,
}

STRING = _type_map["STRING"]
BOOL = _type_map["BOOL"]
BOOLEAN = _type_map["BOOLEAN"]
INT64 = _type_map["INT64"]
INTEGER = _type_map["INTEGER"]
FLOAT64 = _type_map["FLOAT64"]
FLOAT = _type_map["FLOAT"]
TIMESTAMP = _type_map["TIMESTAMP"]
DATETIME = _type_map["DATETIME"]
DATE = _type_map["DATE"]
BYTES = _type_map["BYTES"]
TIME = _type_map["TIME"]
RECORD = _type_map["RECORD"]
NUMERIC = _type_map["NUMERIC"]
BIGNUMERIC = _type_map["NUMERIC"]


class BigQueryExecutionContext(DefaultExecutionContext):
    def create_cursor(self):
        # Set arraysize
        c = super(BigQueryExecutionContext, self).create_cursor()
        if self.dialect.arraysize:
            c.arraysize = self.dialect.arraysize
        return c

    def get_insert_default(self, column):  # pragma: NO COVER
        # Only used by compliance tests
        if isinstance(column.type, Integer):
            return random.randint(-9223372036854775808, 9223372036854775808)  # 1<<63
        elif isinstance(column.type, String):
            return str(uuid.uuid4())

    __remove_type_from_empty_in = _helpers.substitute_string_re_method(
        r"""
        \sIN\sUNNEST\(\[\s               # ' IN UNNEST([ '
        (
        (?:NULL|\(NULL(?:,\sNULL)+\))\)  # '(NULL)' or '((NULL, NULL, ...))'
        \s(?:AND|OR)\s\(1\s!?=\s1        # ' and 1 != 1' or ' or 1 = 1'
        )
        (?:[:][A-Z0-9]+)?                # Maybe ':TYPE' (e.g. ':INT64')
        \s\]\)                           # Close: ' ])'
        """,
        flags=re.IGNORECASE | re.VERBOSE,
        repl=r" IN(\1)",
    )

    @_helpers.substitute_re_method(
        r"""
        \sIN\sUNNEST\(\[\s       # ' IN UNNEST([ '
        (                        # Placeholders. See below.
        %\([^)]+_\d+\)s          # Placeholder '%(foo_1)s'
        (?:,\s                   # 0 or more placeholders
        %\([^)]+_\d+\)s
        )*
        )?
        :([A-Z0-9]+)             # Type ':TYPE' (e.g. ':INT64')
        \s\]\)                   # Close: ' ])'
        """,
        flags=re.IGNORECASE | re.VERBOSE,
    )
    def __distribute_types_to_expanded_placeholders(self, m):
        # If we have an in parameter, it sometimes gets expaned to 0 or more
        # parameters and we need to move the type marker to each
        # parameter.
        # (The way SQLAlchemy handles this is a bit awkward for our
        # purposes.)

        # In the placeholder part of the regex above, the `_\d+
        # suffixes refect that when an array parameter is expanded,
        # numeric suffixes are added.  For example, a placeholder like
        # `%(foo)s` gets expaneded to `%(foo_0)s, `%(foo_1)s, ...`.
        placeholders, type_ = m.groups()
        if placeholders:
            placeholders = placeholders.replace(")", f":{type_})")
        else:
            placeholders = ""
        return f" IN UNNEST([ {placeholders} ])"

    def pre_exec(self):
        self.statement = self.__distribute_types_to_expanded_placeholders(
            self.__remove_type_from_empty_in(self.statement)
        )


class BigQueryCompiler(SQLCompiler):

    compound_keywords = SQLCompiler.compound_keywords.copy()
    compound_keywords[selectable.CompoundSelect.UNION] = "UNION DISTINCT"
    compound_keywords[selectable.CompoundSelect.UNION_ALL] = "UNION ALL"

    def __init__(self, dialect, statement, *args, **kwargs):
        if isinstance(statement, Column):
            kwargs["compile_kwargs"] = util.immutabledict({"include_table": False})
        super(BigQueryCompiler, self).__init__(dialect, statement, *args, **kwargs)

    def visit_insert(self, insert_stmt, asfrom=False, **kw):
        # The (internal) documentation for `inline` is confusing, but
        # having `inline` be true prevents us from generating default
        # primary-key values when we're doing executemany, which seem broken.

        # We can probably do this in the constructor, but I want to
        # make sure this only affects insert, because I'm paranoid. :)

        self.inline = False

        return super(BigQueryCompiler, self).visit_insert(
            insert_stmt, asfrom=False, **kw
        )

    def visit_column(
        self, column, add_to_result_map=None, include_table=True, **kwargs
    ):
        name = orig_name = column.name
        if name is None:
            name = self._fallback_column_name(column)

        is_literal = column.is_literal
        if not is_literal and isinstance(name, elements._truncated_label):
            name = self._truncated_identifier("colident", name)

        if add_to_result_map is not None:
            add_to_result_map(name, orig_name, (column, name, column.key), column.type)

        if is_literal:
            name = self.escape_literal_column(name)
        else:
            name = self.preparer.quote(name, column=True)
        table = column.table
        if table is None or not include_table or not table.named_with_column:
            return name
        else:
            tablename = table.name
            if isinstance(tablename, elements._truncated_label):
                tablename = self._truncated_identifier("alias", tablename)
            return self.preparer.quote(tablename) + "." + name

    def visit_label(self, *args, within_group_by=False, **kwargs):
        # Use labels in GROUP BY clause.
        #
        # Flag set in the group_by_clause method. Works around missing
        # equivalent to supports_simple_order_by_label for group by.
        if within_group_by:
            kwargs["render_label_as_label"] = args[0]
        return super(BigQueryCompiler, self).visit_label(*args, **kwargs)

    def group_by_clause(self, select, **kw):
        return super(BigQueryCompiler, self).group_by_clause(
            select, **kw, within_group_by=True
        )

    ############################################################################
    # Handle parameters in in

    # Due to details in the way sqlalchemy arranges the compilation we
    # expect the bind parameter as an array and unnest it.

    # As it happens, bigquery can handle arrays directly, but there's
    # no way to tell sqlalchemy that, so it works harder than
    # necessary and makes us do the same.

    __sqlalchemy_version_info = tuple(map(int, sqlalchemy.__version__.split(".")))

    __expandng_text = (
        "EXPANDING" if __sqlalchemy_version_info < (1, 4) else "POSTCOMPILE"
    )

    __in_expanding_bind = _helpers.substitute_string_re_method(
        fr"""
        \sIN\s\(                     # ' IN ('
        (
        \[                           # Expanding placeholder
        {__expandng_text}            #   e.g. [EXPANDING_foo_1]
        _[^\]]+                      #
        \]
        (:[A-Z0-9]+)?                # type marker (e.g. ':INT64'
        )
        \)$                          # close w ending )
        """,
        flags=re.IGNORECASE | re.VERBOSE,
        repl=r" IN UNNEST([ \1 ])",
    )

    def visit_in_op_binary(self, binary, operator_, **kw):
        return self.__in_expanding_bind(
            self._generate_generic_binary(binary, " IN ", **kw)
        )

    def visit_empty_set_expr(self, element_types):
        return ""

    def visit_not_in_op_binary(self, binary, operator, **kw):
        return (
            "("
            + self.__in_expanding_bind(
                self._generate_generic_binary(binary, " NOT IN ", **kw)
            )
            + ")"
        )

    visit_notin_op_binary = visit_not_in_op_binary  # before 1.4

    ############################################################################

    ############################################################################
    # Correct for differences in the way that SQLAlchemy escape % and _ (/)
    # and BigQuery does (\\).

    @staticmethod
    def _maybe_reescape(binary):
        binary = binary._clone()
        escape = binary.modifiers.pop("escape", None)
        if escape and escape != "\\":
            binary.right.value = escape.join(
                v.replace(escape, "\\")
                for v in binary.right.value.split(escape + escape)
            )
        return binary

    def visit_contains_op_binary(self, binary, operator, **kw):
        return super(BigQueryCompiler, self).visit_contains_op_binary(
            self._maybe_reescape(binary), operator, **kw
        )

    def visit_notcontains_op_binary(self, binary, operator, **kw):
        return super(BigQueryCompiler, self).visit_notcontains_op_binary(
            self._maybe_reescape(binary), operator, **kw
        )

    def visit_startswith_op_binary(self, binary, operator, **kw):
        return super(BigQueryCompiler, self).visit_startswith_op_binary(
            self._maybe_reescape(binary), operator, **kw
        )

    def visit_notstartswith_op_binary(self, binary, operator, **kw):
        return super(BigQueryCompiler, self).visit_notstartswith_op_binary(
            self._maybe_reescape(binary), operator, **kw
        )

    def visit_endswith_op_binary(self, binary, operator, **kw):
        return super(BigQueryCompiler, self).visit_endswith_op_binary(
            self._maybe_reescape(binary), operator, **kw
        )

    def visit_notendswith_op_binary(self, binary, operator, **kw):
        return super(BigQueryCompiler, self).visit_notendswith_op_binary(
            self._maybe_reescape(binary), operator, **kw
        )

    ############################################################################

    __placeholder = re.compile(r"%\(([^\]:]+)(:[^\]:]+)?\)s$").match

    __expanded_param = re.compile(fr"\(\[" fr"{__expandng_text}" fr"_[^\]]+\]\)$").match

    __remove_type_parameter = _helpers.substitute_string_re_method(
        r"""
        (STRING|BYTES|NUMERIC|BIGNUMERIC)  # Base type
        \(                                 # Dimensions e.g. '(42)', '(4, 2)':
        \s*\d+\s*                          # First dimension
        (?:,\s*\d+\s*)*                    # Remaining dimensions
        \)
        """,
        repl=r"\1",
        flags=re.VERBOSE | re.IGNORECASE,
    )

    def visit_bindparam(
        self,
        bindparam,
        within_columns_clause=False,
        literal_binds=False,
        skip_bind_expression=False,
        **kwargs,
    ):
        param = super(BigQueryCompiler, self).visit_bindparam(
            bindparam,
            within_columns_clause,
            literal_binds,
            skip_bind_expression,
            **kwargs,
        )

        type_ = bindparam.type
        if isinstance(type_, NullType):
            return param

        if (
            isinstance(type_, Numeric)
            and (type_.precision is None or type_.scale is None)
            and isinstance(bindparam.value, Decimal)
        ):
            t = bindparam.value.as_tuple()

            if type_.precision is None:
                type_.precision = len(t.digits)

            if type_.scale is None and t.exponent < 0:
                type_.scale = -t.exponent

        bq_type = self.dialect.type_compiler.process(type_)
        if bq_type[-1] == ">" and bq_type.startswith("ARRAY<"):
            # Values get arrayified at a lower level.
            bq_type = bq_type[6:-1]
        bq_type = self.__remove_type_parameter(bq_type)

        assert_(param != "%s", f"Unexpected param: {param}")

        if bindparam.expanding:
            assert_(self.__expanded_param(param), f"Unexpected param: {param}")
            param = param.replace(")", f":{bq_type})")

        else:
            m = self.__placeholder(param)
            if m:
                name, type_ = m.groups()
                assert_(type_ is None)
                param = f"%({name}:{bq_type})s"

        return param


class BigQueryTypeCompiler(GenericTypeCompiler):
    def visit_INTEGER(self, type_, **kw):
        return "INT64"

    visit_BIGINT = visit_SMALLINT = visit_INTEGER

    def visit_BOOLEAN(self, type_, **kw):
        return "BOOL"

    def visit_FLOAT(self, type_, **kw):
        return "FLOAT64"

    visit_REAL = visit_FLOAT

    def visit_STRING(self, type_, **kw):
        if (type_.length is not None) and isinstance(
            kw.get("type_expression"), Column
        ):  # column def
            return f"STRING({type_.length})"
        return "STRING"

    visit_CHAR = visit_NCHAR = visit_STRING
    visit_VARCHAR = visit_NVARCHAR = visit_TEXT = visit_STRING

    def visit_ARRAY(self, type_, **kw):
        return "ARRAY<{}>".format(self.process(type_.item_type, **kw))

    def visit_BINARY(self, type_, **kw):
        if type_.length is not None:
            return f"BYTES({type_.length})"
        return "BYTES"

    visit_VARBINARY = visit_BINARY

    def visit_NUMERIC(self, type_, **kw):
        if (type_.precision is not None) and isinstance(
            kw.get("type_expression"), Column
        ):  # column def
            if type_.scale is not None:
                suffix = f"({type_.precision}, {type_.scale})"
            else:
                suffix = f"({type_.precision})"
        else:
            suffix = ""

        return (
            "BIGNUMERIC"
            if (type_.precision is not None and type_.precision > 38)
            or (type_.scale is not None and type_.scale > 9)
            else "NUMERIC"
        ) + suffix

    visit_DECIMAL = visit_NUMERIC


class BigQueryDDLCompiler(DDLCompiler):

    # BigQuery has no support for foreign keys.
    def visit_foreign_key_constraint(self, constraint):
        return None

    # BigQuery has no support for primary keys.
    def visit_primary_key_constraint(self, constraint):
        return None

    # BigQuery has no support for unique constraints.
    def visit_unique_constraint(self, constraint):
        return None

    def get_column_specification(self, column, **kwargs):
        colspec = super(BigQueryDDLCompiler, self).get_column_specification(
            column, **kwargs
        )
        if column.comment is not None:
            colspec = "{} OPTIONS(description={})".format(
                colspec, process_string_literal(column.comment)
            )
        return colspec

    def post_create_table(self, table):
        bq_opts = table.dialect_options["bigquery"]
        opts = []

        if ("description" in bq_opts) or table.comment:
            description = process_string_literal(
                bq_opts.get("description", table.comment)
            )
            opts.append(f"description={description}")

        if "friendly_name" in bq_opts:
            opts.append(
                "friendly_name={}".format(
                    process_string_literal(bq_opts["friendly_name"])
                )
            )

        if opts:
            return "\nOPTIONS({})".format(", ".join(opts))

        return ""

    def visit_set_table_comment(self, create):
        table_name = self.preparer.format_table(create.element)
        description = self.sql_compiler.render_literal_value(
            create.element.comment, sqlalchemy.sql.sqltypes.String()
        )
        return f"ALTER TABLE {table_name} SET OPTIONS(description={description})"

    def visit_drop_table_comment(self, drop):
        table_name = self.preparer.format_table(drop.element)
        return f"ALTER TABLE {table_name} SET OPTIONS(description=null)"


def process_string_literal(value):
    return repr(value.replace("%", "%%"))


class BQString(String):
    def literal_processor(self, dialect):
        return process_string_literal


class BQBinary(sqlalchemy.sql.sqltypes._Binary):
    @staticmethod
    def __process_bytes_literal(value):
        return repr(value.replace(b"%", b"%%"))

    def literal_processor(self, dialect):
        return self.__process_bytes_literal


class BQClassTaggedStr(sqlalchemy.sql.type_api.TypeEngine):
    """Type that can get literals via str
    """

    @staticmethod
    def process_literal_as_class_tagged_str(value):
        return f"{value.__class__.__name__.upper()} {repr(str(value))}"

    def literal_processor(self, dialect):
        return self.process_literal_as_class_tagged_str


class BQTimestamp(sqlalchemy.sql.type_api.TypeEngine):
    """Type that can get literals via str
    """

    @staticmethod
    def process_timestamp_literal(value):
        return f"TIMESTAMP {process_string_literal(str(value))}"

    def literal_processor(self, dialect):
        return self.process_timestamp_literal


class BQArray(sqlalchemy.sql.sqltypes.ARRAY):
    def literal_processor(self, dialect):

        item_processor = self.item_type._cached_literal_processor(dialect)
        if not item_processor:
            raise NotImplementedError(
                f"Don't know how to literal-quote values of type {self.item_type}"
            )

        def process_array_literal(value):
            return "[" + ", ".join(item_processor(v) for v in value) + "]"

        return process_array_literal


class BigQueryDialect(DefaultDialect):
    name = "bigquery"
    driver = "bigquery"
    preparer = BigQueryIdentifierPreparer
    statement_compiler = BigQueryCompiler
    type_compiler = BigQueryTypeCompiler
    ddl_compiler = BigQueryDDLCompiler
    execution_ctx_cls = BigQueryExecutionContext
    supports_alter = False
    supports_comments = True
    inline_comments = True
    supports_pk_autoincrement = False
    supports_default_values = False
    supports_empty_insert = False
    supports_multiline_insert = True
    supports_unicode_statements = True
    supports_unicode_binds = True
    supports_native_decimal = True
    description_encoding = None
    supports_native_boolean = True
    supports_simple_order_by_label = True
    postfetch_lastrowid = False
    preexecute_autoincrement_sequences = False

    colspecs = {
        String: BQString,
        sqlalchemy.sql.sqltypes._Binary: BQBinary,
        sqlalchemy.sql.sqltypes.Date: BQClassTaggedStr,
        sqlalchemy.sql.sqltypes.DateTime: BQClassTaggedStr,
        sqlalchemy.sql.sqltypes.Time: BQClassTaggedStr,
        sqlalchemy.sql.sqltypes.TIMESTAMP: BQTimestamp,
        sqlalchemy.sql.sqltypes.ARRAY: BQArray,
    }

    def __init__(
        self,
        arraysize=5000,
        credentials_path=None,
        location=None,
        credentials_info=None,
        *args,
        **kwargs,
    ):
        super(BigQueryDialect, self).__init__(*args, **kwargs)
        self.arraysize = arraysize
        self.credentials_path = credentials_path
        self.credentials_info = credentials_info
        self.location = location
        self.dataset_id = None

    @classmethod
    def dbapi(cls):
        return dbapi

    @staticmethod
    def _build_formatted_table_id(table):
        """Build '<dataset_id>.<table_id>' string using given table."""
        return "{}.{}".format(table.reference.dataset_id, table.table_id)

    @staticmethod
    def _add_default_dataset_to_job_config(job_config, project_id, dataset_id):
        # If dataset_id is set, then we know the job_config isn't None
        if dataset_id:
            # If project_id is missing, use default project_id for the current environment
            if not project_id:
                _, project_id = auth.default()

            job_config.default_dataset = "{}.{}".format(project_id, dataset_id)

    def create_connect_args(self, url):
        (
            project_id,
            location,
            dataset_id,
            arraysize,
            credentials_path,
            default_query_job_config,
        ) = parse_url(url)

        self.arraysize = self.arraysize or arraysize
        self.location = location or self.location
        self.credentials_path = credentials_path or self.credentials_path
        self.dataset_id = dataset_id
        self._add_default_dataset_to_job_config(
            default_query_job_config, project_id, dataset_id
        )
        client = _helpers.create_bigquery_client(
            credentials_path=self.credentials_path,
            credentials_info=self.credentials_info,
            project_id=project_id,
            location=self.location,
            default_query_job_config=default_query_job_config,
        )
        return ([client], {})

    def _json_deserializer(self, row):
        """JSON deserializer for RECORD types.

        The DB-API layer already deserializes JSON to a dictionary, so this
        just returns the input.
        """
        return row

    def _get_table_or_view_names(self, connection, table_type, schema=None):
        current_schema = schema or self.dataset_id
        get_table_name = (
            self._build_formatted_table_id
            if self.dataset_id is None
            else operator.attrgetter("table_id")
        )

        client = connection.connection._client
        datasets = client.list_datasets()

        result = []
        for dataset in datasets:
            if current_schema is not None and current_schema != dataset.dataset_id:
                continue

            try:
                tables = client.list_tables(dataset.reference)
                for table in tables:
                    if table_type == table.table_type:
                        result.append(get_table_name(table))
            except google.api_core.exceptions.NotFound:
                # It's possible that the dataset was deleted between when we
                # fetched the list of datasets and when we try to list the
                # tables from it. See:
                # https://github.com/googleapis/python-bigquery-sqlalchemy/issues/105
                pass
        return result

    @staticmethod
    def _split_table_name(full_table_name):
        # Split full_table_name to get project, dataset and table name
        dataset = None
        table_name = None
        project = None

        table_name_split = full_table_name.split(".")
        if len(table_name_split) == 1:
            table_name = full_table_name
        elif len(table_name_split) == 2:
            dataset, table_name = table_name_split
        elif len(table_name_split) == 3:
            project, dataset, table_name = table_name_split
        else:
            raise ValueError(
                "Did not understand table_name: {}".format(full_table_name)
            )

        return (project, dataset, table_name)

    def _table_reference(
        self, provided_schema_name, provided_table_name, client_project
    ):
        project_id_from_table, dataset_id_from_table, table_id = self._split_table_name(
            provided_table_name
        )
        project_id_from_schema = None
        dataset_id_from_schema = None
        if provided_schema_name is not None:
            provided_schema_name_split = provided_schema_name.split(".")
            if len(provided_schema_name_split) == 1:
                if dataset_id_from_table:
                    project_id_from_schema = provided_schema_name_split[0]
                else:
                    dataset_id_from_schema = provided_schema_name_split[0]
            elif len(provided_schema_name_split) == 2:
                project_id_from_schema = provided_schema_name_split[0]
                dataset_id_from_schema = provided_schema_name_split[1]
            else:
                raise ValueError(
                    "Did not understand schema: {}".format(provided_schema_name)
                )
        if (
            dataset_id_from_schema
            and dataset_id_from_table
            and dataset_id_from_schema != dataset_id_from_table
        ):
            raise ValueError(
                "dataset_id specified in schema and table_name disagree: "
                "got {} in schema, and {} in table_name".format(
                    dataset_id_from_schema, dataset_id_from_table
                )
            )
        if (
            project_id_from_schema
            and project_id_from_table
            and project_id_from_schema != project_id_from_table
        ):
            raise ValueError(
                "project_id specified in schema and table_name disagree: "
                "got {} in schema, and {} in table_name".format(
                    project_id_from_schema, project_id_from_table
                )
            )
        project_id = project_id_from_schema or project_id_from_table or client_project
        dataset_id = dataset_id_from_schema or dataset_id_from_table or self.dataset_id

        table_ref = TableReference.from_string(
            "{}.{}.{}".format(project_id, dataset_id, table_id)
        )
        return table_ref

    def _get_table(self, connection, table_name, schema=None):
        if isinstance(connection, Engine):
            connection = connection.connect()

        client = connection.connection._client

        table_ref = self._table_reference(schema, table_name, client.project)
        try:
            table = client.get_table(table_ref)
        except NotFound:
            raise NoSuchTableError(table_name)
        return table

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
            results += [col]
            if col.field_type == "RECORD":
                cur_columns.append(col)
                fields = [
                    SchemaField.from_api_repr(
                        dict(f.to_api_repr(), name=f"{col.name}.{f.name}")
                    )
                    for f in col.fields
                ]
                results += self._get_columns_helper(fields, cur_columns)
                cur_columns.pop()
        return results

    def get_columns(self, connection, table_name, schema=None, **kw):
        table = self._get_table(connection, table_name, schema)
        columns = self._get_columns_helper(table.schema, [])
        result = []
        for col in columns:
            try:
                coltype = _type_map[col.field_type]
            except KeyError:
                util.warn(
                    "Did not recognize type '%s' of column '%s'"
                    % (col.field_type, col.name)
                )
                coltype = types.NullType

            if col.field_type.endswith("NUMERIC"):
                coltype = coltype(precision=col.precision, scale=col.scale)
            elif col.field_type == "STRING" or col.field_type == "BYTES":
                coltype = coltype(col.max_length)

            result.append(
                {
                    "name": col.name,
                    "type": types.ARRAY(coltype) if col.mode == "REPEATED" else coltype,
                    "nullable": col.mode == "NULLABLE" or col.mode == "REPEATED",
                    "comment": col.description,
                    "default": None,
                    "precision": col.precision,
                    "scale": col.scale,
                    "max_length": col.max_length,
                }
            )

        return result

    def get_table_comment(self, connection, table_name, schema=None, **kw):
        table = self._get_table(connection, table_name, schema)
        return {
            "text": table.description,
        }

    def get_foreign_keys(self, connection, table_name, schema=None, **kw):
        # BigQuery has no support for foreign keys.
        return []

    def get_pk_constraint(self, connection, table_name, schema=None, **kw):
        # BigQuery has no support for primary keys.
        return {"constrained_columns": []}

    def get_indexes(self, connection, table_name, schema=None, **kw):
        table = self._get_table(connection, table_name, schema)
        indexes = []
        if table.time_partitioning:
            indexes.append(
                {
                    "name": "partition",
                    "column_names": [table.time_partitioning.field],
                    "unique": False,
                }
            )
        if table.clustering_fields:
            indexes.append(
                {
                    "name": "clustering",
                    "column_names": table.clustering_fields,
                    "unique": False,
                }
            )
        return indexes

    def get_schema_names(self, connection, **kw):
        if isinstance(connection, Engine):
            connection = connection.connect()

        datasets = connection.connection._client.list_datasets()
        return [d.dataset_id for d in datasets]

    def get_table_names(self, connection, schema=None, **kw):
        if isinstance(connection, Engine):
            connection = connection.connect()

        return self._get_table_or_view_names(connection, "TABLE", schema)

    def get_view_names(self, connection, schema=None, **kw):
        if isinstance(connection, Engine):
            connection = connection.connect()

        return self._get_table_or_view_names(connection, "VIEW", schema)

    def do_rollback(self, dbapi_connection):
        # BigQuery has no support for transactions.
        pass

    def _check_unicode_returns(self, connection, additional_tests=None):
        # requests gives back Unicode strings
        return True

    def get_view_definition(self, connection, view_name, schema=None, **kw):
        if isinstance(connection, Engine):
            connection = connection.connect()
        client = connection.connection._client
        if self.dataset_id:
            view_name = f"{self.dataset_id}.{view_name}"
        view = client.get_table(view_name)
        return view.view_query


try:
    import alembic  # noqa
except ImportError:
    pass
else:
    from alembic.ddl import impl

    class PyBigQueryImpl(impl.DefaultImpl):
        __dialect__ = "bigquery"
