# Copyright (c) 2021 The sqlalchemy-bigquery Authors
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

from typing import Mapping, Tuple

import packaging.version
import sqlalchemy.sql.default_comparator
import sqlalchemy.sql.sqltypes
import sqlalchemy.types

from . import base

sqlalchemy_1_4_or_more = packaging.version.parse(
    sqlalchemy.__version__
) >= packaging.version.parse("1.4")

if sqlalchemy_1_4_or_more:
    import sqlalchemy.sql.coercions
    import sqlalchemy.sql.roles

# We have to delay getting the type compiler, because of circular imports. :(
type_compiler = None


class STRUCT(sqlalchemy.sql.sqltypes.Indexable, sqlalchemy.types.UserDefinedType):
    """
    A type for BigQuery STRUCT/RECORD data

    See https://googleapis.dev/python/sqlalchemy-bigquery/latest/struct.html
    """

    def __init__(
        self,
        *fields: Tuple[str, sqlalchemy.types.TypeEngine],
        **kwfields: Mapping[str, sqlalchemy.types.TypeEngine],
    ):
        self.__fields = tuple(
            (
                name,
                type_ if isinstance(type_, sqlalchemy.types.TypeEngine) else type_(),
            )
            for (name, type_) in (fields + tuple(kwfields.items()))
        )

        self.__byname = {name.lower(): type_ for (name, type_) in self.__fields}

    def __repr__(self):
        fields = ", ".join(f"{name}={repr(type_)}" for name, type_ in self.__fields)
        return f"STRUCT({fields})"

    def get_col_spec(self, **kw):
        global type_compiler

        try:
            process = type_compiler.process
        except AttributeError:
            type_compiler = base.dialect.type_compiler(base.dialect())
            process = type_compiler.process

        fields = ", ".join(f"{name} {process(type_)}" for name, type_ in self.__fields)
        return f"STRUCT<{fields}>"

    def bind_processor(self, dialect):
        return dict

    class Comparator(sqlalchemy.sql.sqltypes.Indexable.Comparator):
        def _setup_getitem(self, name):
            if not isinstance(name, str):
                raise TypeError(
                    f"STRUCT fields can only be accessed with strings field names,"
                    f" not {name}."
                )
            subtype = self.expr.type._STRUCT__byname.get(name.lower())
            if subtype is None:
                raise KeyError(name)
            operator = struct_getitem_op
            index = _field_index(self, name, operator)
            return operator, index, subtype

        def __getattr__(self, name):
            if name.lower() in self.expr.type._STRUCT__byname:
                return self[name]

    comparator_factory = Comparator


if sqlalchemy_1_4_or_more:

    def _field_index(self, name, operator):
        return sqlalchemy.sql.coercions.expect(
            sqlalchemy.sql.roles.BinaryElementRole,
            name,
            expr=self.expr,
            operator=operator,
            bindparam_type=sqlalchemy.types.String(),
        )


else:

    def _field_index(self, name, operator):
        return sqlalchemy.sql.default_comparator._check_literal(
            self.expr, operator, name, bindparam_type=sqlalchemy.types.String(),
        )


def struct_getitem_op(a, b):
    raise NotImplementedError()


sqlalchemy.sql.default_comparator.operator_lookup[
    struct_getitem_op.__name__
] = sqlalchemy.sql.default_comparator.operator_lookup["json_getitem_op"]


class SQLCompiler:
    def visit_struct_getitem_op_binary(self, binary, operator_, **kw):
        left = self.process(binary.left, **kw)
        return f"{left}.{binary.right.value}"
