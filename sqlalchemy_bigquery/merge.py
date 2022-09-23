# Copyright 2021 Google LLC
#
# Use of this source code is governed by an MIT-style
# license that can be found in the LICENSE file or at
# https://opensource.org/licenses/MIT.

import textwrap
from copy import copy
from dataclasses import dataclass
from typing import Any, ClassVar, Dict, Optional, Tuple, Type, Union

import sqlalchemy as sa
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.sql.compiler import SQLCompiler
from sqlalchemy.sql.expression import ClauseElement, ColumnElement
from sqlalchemy.sql.selectable import Subquery  # noqa
from sqlalchemy.sql.selectable import Alias, Select, TableClause

MergeConditionType = Optional[ColumnElement[sa.Boolean]]


class Merge(ClauseElement):
    def __init__(
        self,
        target: Union[Alias, TableClause],
        source: Union[Alias, TableClause, Subquery, Select],
        *,
        on: ColumnElement[sa.Boolean],
    ):
        super().__init__()
        if not (
            isinstance(target, TableClause)
            or (
                isinstance(target, Alias)
                and isinstance(getattr(target, "element"), TableClause)
            )
        ):
            raise Exception(
                "Parameter `target` must be a table, or an aliased table,"
                f" instead received:\n{repr(target)}"
            )
        if not isinstance(target, (Alias, TableClause, Subquery, Select)):
            raise Exception(
                "Parameter `source` must be a table, alias, subquery, or selectable,"
                f" instead received:\n{repr(target)}"
            )

        self.when = tuple()
        self.target = target
        self.source = source
        self.on = on
        self.when: Tuple[WhenBase] = tuple()

    def when_matched(self, condition: MergeConditionType = None):
        return MergeWhenMatched(self, condition)

    def when_matched_not_matched_by_target(self, condition: MergeConditionType = None):
        return MergeWhenNotMatchedByTarget(self, condition)

    def when_matched_not_matched_by_source(self, condition: MergeConditionType = None):
        return MergeWhenNotMatchedBySource(self, condition)

    def _add_when(self, when: "WhenBase"):
        cloned = copy(self)
        cloned.when = tuple(cloned.when) + (when,)
        return cloned


class ThenBase:
    pass


@dataclass(frozen=True)
class ThenUpdate(ThenBase):
    fields: Dict[str, ColumnElement[Any]]


@dataclass(frozen=True)
class ThenInsert(ThenBase):
    fields: Dict[str, ColumnElement[Any]]


@dataclass(frozen=True)
class ThenDelete(ThenBase):
    pass


class WhenBase:
    then: ThenBase
    condition: MergeConditionType


@dataclass(frozen=True)
class WhenMatched(WhenBase):
    then: Union[ThenUpdate, ThenDelete]
    condition: MergeConditionType = None


@dataclass(frozen=True)
class WhenNotMatchedByTarget(WhenBase):
    then: ThenInsert
    condition: MergeConditionType = None


@dataclass(frozen=True)
class WhenNotMatchedBySource(WhenBase):
    then: Union[ThenUpdate, ThenDelete]
    condition: MergeConditionType = None


class MergeThenUpdateDeleteBase:
    merge_cls: ClassVar[Union[Type[WhenMatched], Type[WhenNotMatchedBySource]]]

    def __init__(self, stmt: "Merge", condition: MergeConditionType = None) -> None:
        super().__init__()
        self.stmt = stmt
        self.condition = condition

    def then_update(self, fields: Dict[str, ColumnElement[Any]]):
        return self.stmt._add_when(self.merge_cls(ThenUpdate(fields), self.condition))

    def then_delete(self):
        return self.stmt._add_when(self.merge_cls(ThenDelete(), self.condition))


class MergeWhenMatched(MergeThenUpdateDeleteBase):
    merge_cls = WhenMatched


class MergeWhenNotMatchedBySource(MergeThenUpdateDeleteBase):
    merge_cls = WhenNotMatchedBySource


class MergeWhenNotMatchedByTarget:
    def __init__(self, stmt: "Merge", condition: MergeConditionType = None) -> None:
        super().__init__()
        self.stmt = stmt
        self.condition = condition

    def then_insert(self, fields: Dict[str, ColumnElement[Any]]):
        return self.stmt._add_when(
            WhenNotMatchedByTarget(ThenInsert(fields), self.condition)
        )


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

    target = _compile_select(self.target)
    source = _compile_select(self.source)
    on = compiler.process(self.on, **kwargs)

    code = f"MERGE\n    INTO{target}"
    code += f"\n    USING{source}"
    code += f"\n    ON {on}"
    for when in self.when:
        code += "\n" + compiler.process(when, **kwargs)
    return code.rstrip() + ";"


@compiles(WhenMatched)
def _compile_when_matched(self: WhenMatched, compiler: SQLCompiler, **kwargs):
    return "WHEN MATCHED" + _compile_when(self, compiler, **kwargs)


@compiles(WhenNotMatchedByTarget)
def _compile_when_not_matched_by_target(
    self: WhenNotMatchedByTarget, compiler: SQLCompiler, **kwargs
):
    return "WHEN NOT MATCHED BY TARGET" + _compile_when(self, compiler, **kwargs)


@compiles(WhenNotMatchedBySource)
def _compile_when_not_matched_by_source(
    self: WhenNotMatchedBySource, compiler: SQLCompiler, **kwargs
):
    return "WHEN MATCHED NOT MATCHED BY SOURCE" + _compile_when(
        self, compiler, **kwargs
    )


def _compile_when(when: WhenBase, compiler: SQLCompiler, **kwargs):
    code = ""
    if when.condition is not None:
        code += "\n    AND "
        code += _indent(compiler.process(when.condition, **kwargs)).strip()
    code += "\n    THEN "
    code += _indent(compiler.process(when.then, **kwargs)).strip()
    return code


@compiles(ThenUpdate)
def _compile_then_update(self: ThenUpdate, compiler: SQLCompiler, **kwargs):
    code = "UPDATE SET"
    code += ",".join(
        "\n    " + _indent(f"{key} = {compiler.process(value, **kwargs)}", 2).strip()
        for key, value in self.fields.items()
    )
    return code


@compiles(ThenInsert)
def _compile_then_insert(self: ThenInsert, compiler: SQLCompiler, **kwargs):
    code = "INSERT ("
    code += ",".join(f"\n    {key}" for key in self.fields.keys())
    code += "\n) VALUES ("
    code += ",".join(
        "\n    " + _indent(compiler.process(value, **kwargs), 2).strip()
        for value in self.fields.values()
    )
    code += "\n)"
    return code


@compiles(ThenDelete)
def _compile_then_delete(self: ThenDelete, compiler: SQLCompiler, **kwargs):
    return "DELETE"


def _indent(text: str, amount: int = 1) -> str:
    return textwrap.indent(text, prefix="    " * amount)
