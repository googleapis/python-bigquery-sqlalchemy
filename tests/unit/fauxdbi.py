import google.api_core.exceptions
import google.cloud.bigquery.schema
import google.cloud.bigquery.table
import google.cloud.bigquery.dbapi.cursor
import contextlib
import datetime
import decimal
import re
import sqlite3


class Connection:
    def __init__(self, connection, test_data, client, *args, **kw):
        self.connection = connection
        self.test_data = test_data
        self._client = client
        client.connection = self

    def cursor(self):
        return Cursor(self)

    def commit(self):
        pass

    def rollback(self):
        pass

    def close(self):
        self.connection.close()


class Cursor:

    arraysize = 1

    def __init__(self, connection):
        self.connection = connection
        self.cursor = connection.connection.cursor()

    def execute(self, operation, parameters=()):
        self.connection.test_data["execute"].append((operation, parameters))
        operation, types_ = google.cloud.bigquery.dbapi.cursor._extract_types(operation)
        if parameters:
            operation, parameters = self._convert_params(operation, parameters)
            parameters = [
                float(p) if isinstance(p, decimal.Decimal) else p for p in parameters
            ]
            parameters = [
                str(p)
                if isinstance(p, (datetime.date, datetime.time, datetime.datetime))
                else p
                for p in parameters
            ]

        for prefix in "DATETIME", "DATE", "TIMESTAMP", "TIME":
            operation = operation.replace(prefix + " ", "")

        operation = re.sub("(, |[(])b(['\"])", r"\1\2", operation)

        operation = self.__handle_comments(operation)

        self.cursor.execute(operation, parameters)
        self.description = self.cursor.description
        self.rowcount = self.cursor.rowcount

    __alter_table = re.compile(
        r"\s*ALTER\s+TABLE\s+`(?P<table>\w+)`\s+"
        r"SET\s+OPTIONS\(description=(?P<comment>[^)]+)\)",
        re.I).match
    __create_table = re.compile(r"\s*create\s+table\s+`(?P<table>\w+)`", re.I).match
    __options = re.compile(
        r"(?P<prefix>`(?P<col>\w+)`\s+\w+|\))"
        r"\s+options\((?P<options>[^)]+)\)",
        re.I)

    def __handle_comments(self, operation):
        m = self.__create_table(operation)
        if m:
            table_name = m.group('table')

            def repl(m):
                col = m.group('col') or ''
                options = {
                    name.strip().lower(): value.strip()
                    for name, value in (
                        o.split('=')
                        for o in m.group('options').split(',')
                        )
                    }

                comment = options.get('description')
                if comment:
                    self.cursor.execute(
                        f"insert into comments values(?, {comment})"
                        f" on conflict(key) do update set comment=excluded.comment",
                        [table_name + ',' +  col],
                        )

                return m.group('prefix')

            return self.__options.sub(repl, operation)

        m = self.__alter_table(operation)
        if m:
            table_name = m.group('table')
            comment = m.group('comment')
            return (f"insert into comments values({repr(table_name + ',')}, {comment})"
                    f" on conflict(key) do update set comment=excluded.comment"
                    )

        return operation

    @staticmethod
    def _convert_params(operation, parameters):
        ordered_parameters = []

        def repl(m):
            name = m.group(1)
            ordered_parameters.append(parameters[name])
            return "?"

        operation = re.sub("%\((\w+)\)s", repl, operation)
        return operation, ordered_parameters

    def executemany(self, operation, parameters_list):
        for parameters in parameters_list:
            self.execute(operation, parameters)

    def close(self):
        self.cursor.close()

    def _fix_binary(self, row):
        if row is None:
            return row

        return [
            v.encode("utf8")
            if "BINARY" in d[0].upper() and not isinstance(v, bytes)
            else v
            for d, v in zip(self.description, row)
        ]

    def fetchone(self):
        return self._fix_binary(self.cursor.fetchone())


class attrdict(dict):
    def __setattr__(self, name, val):
        self[name] = val
    def __getattr__(self, name):
        if name not in self:
            self[name] = attrdict()
        return self[name]


class FauxClient:

    def __init__(
        self,
        project=None,
        default_query_job_config=None,
        *args,
        **kw
        ):

        if project is None:
            if default_query_job_config is not None:
                project = default_query_job_config.default_dataset.project
            else:
                project = 'authproj'  # we would still have gotten it from auth.

        self.project = project
        self.tables = attrdict()

    @staticmethod
    def _row_dict(row, cursor):
        result = {d[0]: value for d, value in zip(cursor.description, row)}
        return result

    def _get_field(
        self, type, name=None, notnull=None, mode=None, description=None, fields=(),
        columns=None, **_
    ):
        if columns:
            custom = columns.get(name)
            if custom:
                return self._get_field(
                    **dict(name=name, type=type, notnull=notnull, **custom)
                )

        if not mode:
            mode="REQUIRED" if notnull else "NULLABLE"

        field = google.cloud.bigquery.schema.SchemaField(
            name=name,
            field_type=type,
            mode=mode,
            description=description,
            fields=tuple(self._get_field(**f) for f in fields),
            )

        return field

    def __get_comments(self, cursor, table_name):
        cursor.execute(
            f"select key, comment"
            f" from comments where key like {repr(table_name + '%')}")

        return {key.split(',')[1]: comment for key, comment in cursor}

    def get_table(self, table_ref):
        table_ref = google.cloud.bigquery.table._table_arg_to_table_ref(
            table_ref, self.project)
        table_name = table_ref.table_id
        with contextlib.closing(self.connection.connection.cursor()) as cursor:
            cursor.execute(f"select * from sqlite_master where name='{table_name}'")
            rows = list(cursor)
            if rows:
                table_data = self._row_dict(rows[0], cursor)

                comments = self.__get_comments(cursor, table_name)
                table_comment = comments.pop('', None)
                columns = getattr(self.tables, table_name).columns
                for col, comment in comments.items():
                    getattr(columns, col).description = comment

                cursor.execute(f"PRAGMA table_info('{table_name}')")
                schema = [
                    self._get_field(columns=columns, **self._row_dict(row, cursor))
                    for row in cursor
                ]
                table = google.cloud.bigquery.table.Table(table_ref, schema)
                table.description = table_comment
                if table_data['type'] == 'view' and table_data['sql']:
                    table.view_query = table_data['sql'][
                        table_data['sql'].lower().index('select'):]

                for aname, value in self.tables.get(table_name, {}).items():
                    setattr(table, aname, value)

                return table
            else:
                raise google.api_core.exceptions.NotFound(table_ref)

    def list_datasets(self):
        return [google.cloud.bigquery.Dataset("myproject.mydataset"),
                google.cloud.bigquery.Dataset("myproject.yourdataset"),
                ]

    def list_tables(self, dataset):
        with contextlib.closing(self.connection.connection.cursor()) as cursor:
            cursor.execute(f"select * from sqlite_master")
            return [
                google.cloud.bigquery.table.TableListItem(
                    dict(
                        tableReference=dict(
                            projectId=dataset.project,
                            datasetId=dataset.dataset_id,
                            tableId=row['name'],
                            ),
                        type=row['type'].upper(),
                        )
                    )
                for row in (
                        self._row_dict(row, cursor)
                        for row in cursor
                        )
                if row['name'] != 'comments'
                ]
