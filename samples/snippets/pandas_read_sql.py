#!/usr/bin/env python
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


def read_data_into_pandas_using_read_sql() -> None:
    # [START sqlalchemy_bigquery_read_sql]

    #Read BigQuery data using the pandas `read_sql` function.

    import pandas as pd
    from sqlalchemy import create_engine

    # Use project from credentials provided by your environment.
    # https://googleapis.dev/python/google-api-core/latest/auth.html
    engine = create_engine("bigquery://")

    # Read data from the austin 311-complaint table in the google
    # public datasets.

    # The table is given in three parts in this example:
    # PROJECT.DATASET.TABLE

    df = pd.read_sql(
        """select created_date, complaint_description
           from bigquery-public-data.austin_311.311_service_requests
           limit 10
        """,
        engine,
    )

    print(df)
    # [END sqlalchemy_bigquery_read_sql]


if __name__ == "__main__":
    read_data_into_pandas_using_read_sql()
