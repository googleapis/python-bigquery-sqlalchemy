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

from alembic import op
from alembic.testing.fixtures import op_fixture
from alembic.testing.fixtures import TestBase
from sqlalchemy import Integer


class OpTest(TestBase):

    def test_alter_table_rename_oracle(self):
        context = op_fixture("bigquery")
        op.rename_table("s", "t")
        context.assert_("ALTER TABLE s RENAME COLUMN TO t")

    def test_alter_column_new_type(self):
        context = op_fixture("bigquery")
        op.alter_column("t", "c", type_=Integer)
        context.assert_("ALTER TABLE t SET DATA TYPE c INTEGER")
