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

from sqlalchemy.testing.plugin.pytestplugin import *

import google.cloud.bigquery.dbapi.connection
import pybigquery.sqlalchemy_bigquery
import sqlalchemy
import traceback

google.cloud.bigquery.dbapi.connection.Connection.rollback = lambda self: None


def visit_delete(self, delete_stmt, *args, **kw):
    if delete_stmt._whereclause is None:
        if 'teardown' in set(f.name for f in traceback.extract_stack()):
            delete_stmt._whereclause = sqlalchemy.true()
            return super(pybigquery.sqlalchemy_bigquery.BigQueryCompiler, self
                         ).visit_delete(delete_stmt, *args, **kw)
        else:
            breakpoint()


pybigquery.sqlalchemy_bigquery.BigQueryCompiler.visit_delete = visit_delete
