# Copyright 2021 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

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
