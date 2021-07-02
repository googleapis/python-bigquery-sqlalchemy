# Copyright (c) 2017 The PyBigQuery Authors
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

from .version import __version__

import warnings
import sys
import sqlalchemy_bigquery
from sqlalchemy_bigquery import api, base, _helpers, parse_url, requirements

sys.modules[__name__ + ".sqlalchemy_bigquery"] = base
sys.modules["sqlalchemy_bigquery.sqlalchemy_bigquery"] = base
sqlalchemy_bigquery.sqlalchemy_bigquery = base
sqlalchemy_bigquery = base
del base

for module in api, _helpers, parse_url, requirements:
    sys.modules[module.__name__.replace("sqlalchemy_bigquery", "pybigquery")] = module

__all__ = ("__version__",)

warnings.warn(
    "pybigquery is deprecated. Use sqlalchemy-bigquery instead.",
    DeprecationWarning,
    stacklevel=2,
)
