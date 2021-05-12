# Copyright (c) 2021 The PyBigQuery Authors
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

import re
import traceback

import sqlalchemy
from sqlalchemy.testing.plugin.pytestplugin import *  # noqa

import google.cloud.bigquery.dbapi.connection
import pybigquery.sqlalchemy_bigquery

pybigquery.sqlalchemy_bigquery.BigQueryDialect.preexecute_autoincrement_sequences = True
google.cloud.bigquery.dbapi.connection.Connection.rollback = lambda self: None

_where = re.compile(r"\s+WHERE\s+", re.IGNORECASE).search

# BigQuery requires delete statements to have where clauses. Other
# databases don't and sqlalchemy doesn't include where clauses when
# cleaning up test data.  So we add one when we see a delete without a
# where clause when tearing down tests.  We only do this during tear
# down, by inspecting the stack, because we don't want to hide bugs
# outside of test house-keeping.


def visit_delete(self, delete_stmt, *args, **kw):
    text = super(pybigquery.sqlalchemy_bigquery.BigQueryCompiler, self).visit_delete(
        delete_stmt, *args, **kw
    )

    if not _where(text) and any(
        "teardown" in f.name.lower() for f in traceback.extract_stack()
    ):
        text += " WHERE true"

    return text


pybigquery.sqlalchemy_bigquery.BigQueryCompiler.visit_delete = visit_delete
