from enum import auto, Enum
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
        super_proc = self.bind_processor(dialect)

        def process(value):
            value = super_proc(value)
            return repr(value)

        return process

    class Comparator(sqltypes.JSON.Comparator):
        def _generate_converter(self, name, lax):
            prefix = "LAX_" if lax else ""
            func_ = getattr(sqlalchemy.func, f"{prefix}{name}")
            return func_

        def as_boolean(self, lax=False):
            func_ = self._generate_converter("BOOL", lax)
            return func_(self.expr, type_=sqltypes.Boolean)

        def as_string(self, lax=False):
            func_ = self._generate_converter("STRING", lax)
            return func_(self.expr, type_=sqltypes.String)

        def as_integer(self, lax=False):
            func_ = self._generate_converter("INT64", lax)
            return func_(self.expr, type_=sqltypes.Integer)

        def as_float(self, lax=False):
            func_ = self._generate_converter("FLOAT64", lax)
            return func_(self.expr, type_=sqltypes.Float)

        def as_numeric(self, precision, scale, asdecimal=True):
            # No converter available in BigQuery
            raise NotImplementedError()

    comparator_factory = Comparator

    class JSONPathMode(Enum):
        LAX = auto()
        LAX_RECURSIVE = auto()


class JSONPathType(_FormatTypeMixin, sqltypes.JSON.JSONPathType):
    def _mode_prefix(self, mode):
        if mode == JSON.JSONPathMode.LAX:
            mode_prefix = "lax "
        elif mode == JSON.JSONPathMode.LAX_RECURSIVE:
            mode_prefix = "lax recursive"
        else:
            raise NotImplementedError(f"Unhandled JSONPathMode: {mode}")
        return mode_prefix

    def _format_value(self, value):
        if isinstance(value[0], JSON.JSONPathMode):
            mode = value[0]
            mode_prefix = self._mode_prefix(mode)
            value = value[1:]
        else:
            mode_prefix = ""

        return "%s$%s" % (
            mode_prefix,
            "".join(
                [
                    "[%s]" % elem if isinstance(elem, int) else '."%s"' % elem
                    for elem in value
                ]
            ),
        )
