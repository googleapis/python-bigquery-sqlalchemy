# Copyright 2021 The PyBigQuery Authors
#
# Use of this source code is governed by an MIT-style
# license that can be found in the LICENSE file or at
# https://opensource.org/licenses/MIT.

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
