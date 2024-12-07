import json
import sqlalchemy
from sqlalchemy.sql import sqltypes


class _FormatTypeMixin:
    def _format_value(self, value):
        raise NotImplementedError()

    def bind_processor(self, dialect):
        super_proc = self.string_bind_processor(dialect)

        def process(value):
            value = self._format_value(value)
            if super_proc:
                value = super_proc(value)
            return value

        return process

    def literal_processor(self, dialect):
        super_proc = self.string_literal_processor(dialect)

        def process(value):
            value = self._format_value(value)
            if super_proc:
                value = super_proc(value)
            return value

        return process


class JSON(sqltypes.JSON):
    def bind_expression(self, bindvalue):
        # JSON query parameters are STRINGs
        return sqlalchemy.func.PARSE_JSON(bindvalue, type_=self)

    def literal_processor(self, dialect):
        json_serializer = dialect._json_serializer or json.dumps

        def process(value):
            value = json_serializer(value)
            return f"'{value}'"

        return process


class JSONPathType(_FormatTypeMixin, sqltypes.JSON.JSONPathType):
    # TODO: Handle lax, lax recursive
    def _format_value(self, value):
        return "$%s" % (
            "".join(
                [
                    "[%s]" % elem if isinstance(elem, int) else '."%s"' % elem
                    for elem in value
                ]
            )
        )


