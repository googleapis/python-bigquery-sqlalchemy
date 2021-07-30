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


def read_geographic_data_into_pandas_using_read_sql() -> None:
    """Read BigQuery geographic data using the geopandas `read_postgis` function.

    Even though, as it's name implies, `read_postgis` was designed to
    work with the Postgres GIS extension, PostGIS, it works with
    BigQuery too, as long as the data returned from queries is in `WKB
    format
    <https://en.wikipedia.org/wiki/Well-known_text_representation_of_geometry>`_.
    """
    import geopandas
    import pandas as pd
    from sqlalchemy import create_engine

    # Use project from credentials provided by your environment.
    # https://googleapis.dev/python/google-api-core/latest/auth.html
    engine = create_engine("bigquery://")

    # Read data from the austin 311-complaint table in the google
    # public datasets.

    # The table is given in three parts in this example:
    # PROJECT.DATASET.TABLE

    # The table has latitude and longitude columns. We can use those
    # to construct point locations for complaints.  We'll use the
    # `ST_GEOGPOINT` function to construct points, and the `ST_ASBINARY`
    # function to retrieve data in WKB format.
    # For information on available functions, see:
    # https://cloud.google.com/bigquery/docs/reference/standard-sql/geography_functions

    # The last argument, `'EPSG:4326'`, tells geopandas that the data
    # are in the EPSG:4326 coordinate system, which is the coordinate
    # system used by BigQuery.

    df = geopandas.read_postgis(
        """select created_date, complaint_description,
                  ST_ASBINARY(ST_GEOGPOINT(longitude, latitude)) as location
                  from bigquery-public-data.austin_311.311_service_requests
           limit 10
        """,
        engine,
        "location",
        'EPSG:4326',
    )

    # Don't wrap pr elide:
    pd.options.display.max_columns = 2000
    pd.options.display.width = 2000

    print(df)


if __name__ == "__main__":
    read_geographic_data_into_pandas_using_read_sql()
