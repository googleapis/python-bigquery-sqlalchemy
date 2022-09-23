# Copyright 2021 Google LLC
#
# Use of this source code is governed by an MIT-style
# license that can be found in the LICENSE file or at
# https://opensource.org/licenses/MIT.

import textwrap
from dataclasses import dataclass
from typing import Any, ClassVar, Optional, Type, Union

import sqlalchemy as sa
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.sql.compiler import SQLCompiler
from sqlalchemy.sql.expression import ClauseElement, ColumnElement
from sqlalchemy.sql.selectable import Subquery  # noqa
from sqlalchemy.sql.selectable import Alias, Select, TableClause


class MergeThen:
    pass


@dataclass(frozen=True)
class MergeThenUpdate(MergeThen):
    fields: dict[str, ColumnElement[Any]]


@dataclass(frozen=True)
class MergeThenInsert(MergeThen):
    fields: dict[str, ColumnElement[Any]]


@dataclass(frozen=True)
class MergeThenDelete(MergeThen):
    pass


class Merge(ClauseElement):
    Then: ClassVar[Type[MergeThen]] = MergeThen
    ThenInsert: ClassVar[Type[MergeThenInsert]] = MergeThenInsert
    ThenUpdate: ClassVar[Type[MergeThenUpdate]] = MergeThenUpdate
    ThenDelete: ClassVar[Type[MergeThenDelete]] = MergeThenDelete

    def __init__(
        self,
        *,
        into: Union[Alias, TableClause],
        using: Union[Alias, TableClause, Subquery, Select],
        on: ColumnElement[sa.Boolean],
        when_matched_and: Optional[ColumnElement[sa.Boolean]] = None,
        when_matched: Optional["MergeThen"] = None,
        when_not_matched: Optional["MergeThen"] = None,
        when_not_matched_by_source: Optional["MergeThen"] = None,
    ):
        super().__init__()
        if not (
            isinstance(into, TableClause)
            or (
                isinstance(into, Alias)
                and isinstance(getattr(into, "element"), TableClause)
            )
        ):
            raise Exception(
                "Parameter `into` must be a table, or an aliased table,"
                f" instead received:\n{repr(into)}"
            )
        if not isinstance(into, (Alias, TableClause, Subquery, Select)):
            raise Exception(
                "Parameter `using` must be a table, alias, subquery, or selectable,"
                f" instead received:\n{repr(into)}"
            )

        if when_matched is None and when_matched_and is not None:
            raise TypeError(f"Must supply `when_matched` if `when_matched_and` is set.")
        if when_matched_and is None and when_matched is not None:
            raise TypeError(f"Must supply `when_matched_and` if `when_matched` is set.")

        self.into = into
        self.using = using
        self.on = on
        self.when_matched_and = when_matched_and
        self.when_matched = when_matched
        self.when_not_matched = when_not_matched
        self.when_not_matched_by_source = when_not_matched_by_source


@compiles(MergeThenUpdate)
def _compile_merge_then_update(self: MergeThenUpdate, compiler: SQLCompiler, **kwargs):
    code = "UPDATE SET"
    code += ",".join(
        "\n    " + _indent(f"{key} = {compiler.process(value, **kwargs)}", 2).strip()
        for key, value in self.fields.items()
    )
    return code


@compiles(MergeThenInsert)
def _compile_merge_then_insert(self: MergeThenInsert, compiler: SQLCompiler, **kwargs):
    code = "INSERT ("
    code += ",".join(f"\n    {key}" for key in self.fields.keys())
    code += "\n) VALUES ("
    code += ",".join(
        "\n    " + _indent(compiler.process(value, **kwargs), 2).strip()
        for value in self.fields.values()
    )
    code += "\n)"
    return code


@compiles(MergeThenDelete)
def _compile_merge_then_delete(self: MergeThenDelete, compiler: SQLCompiler, **kwargs):
    return "DELETE"


@compiles(Merge)
def _compile_merge(self: Merge, compiler: SQLCompiler, **kwargs):
    def _compile_select(value):
        if isinstance(value, (TableClause, Alias, Subquery)):
            select = sa.select([]).select_from(value)
            code = compiler.process(select, **kwargs).split("FROM", 1)[1]
        else:
            code = compiler.process(value, **kwargs)
        code = code.strip()
        if "\n" not in code:
            return " " + code
        return "\n" + _indent(code, 2)

    into = _compile_select(self.into)
    using = _compile_select(self.using)
    on = compiler.process(self.on, **kwargs)
    code = f"MERGE\n    INTO{into}"
    code += f"\n    USING{using}"
    code += f"\n    ON {on}"

    if self.when_matched is not None:
        when_matched = _indent(compiler.process(self.when_matched, **kwargs))
        if self.when_matched_and is not None:
            when_matched_and = _indent(
                compiler.process(self.when_matched_and, **kwargs), 2
            )
            code += (
                f"\nWHEN MATCHED\n    AND (\n{when_matched_and}\n    )\n    THEN\n"
                + when_matched
            )
        else:
            code += f"\nWHEN MATCHED THEN\n" + when_matched

    if self.when_not_matched is not None:
        when_not_matched = _indent(compiler.process(self.when_not_matched, **kwargs))
        code += "\nWHEN NOT MATCHED\n    THEN\n" + when_not_matched

    if self.when_not_matched_by_source is not None:
        when_not_matched_by_source = _indent(
            compiler.process(self.when_not_matched_by_source, **kwargs)
        )
        code += "\nWHEN NOT MATCHED BY SOURCE\n    THEN\n" + when_not_matched_by_source

    return code.rstrip() + ";"


def _indent(text: str, amount: int = 1) -> str:
    return textwrap.indent(text, prefix="    " * amount)
