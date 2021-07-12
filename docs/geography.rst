Working with Geographic data
============================

BigQuery provides a `GEOGRAPHY data type
<https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types#geography_type>`_
for `working with geographic data
<https://cloud.google.com/bigquery/docs/gis-data>`_, including:

- Points,
- Linestrings,
- Polygons, and
- Collections of points, linestrings, and polygons.

Geographic data uses the `WGS84
<https://earth-info.nga.mil/#tab_wgs84-data>`_ coordinate system.

To define a geography column, use the `GEOGRAPHY` data type imported
from the `sqlalchemy_bigquery` package::

  from sqlalchemy.ext.declarative import declarative_base
  from sqlalchemy import Column, String
  from sqlalchemy_bigquery import GEOGRAPHY

  Base = declarative_base()

  class Lake(Base):
      __tablename__ = 'lakes'

      name = Column(String)
      geog = column(GEOGRAPHY)

BigQuery has a variety of `SQL geographic functions
<https://cloud.google.com/bigquery/docs/reference/standard-sql/geography_functions>`_
for working with geographic data.  Among these are functions for
converting between SQL geometry objects and `standard text (WKT) and
binary (WKB) representations
<https://en.wikipedia.org/wiki/Well-known_text_representation_of_geometry>`_.

Geography data is typically represented in Python as text strings in
WKT format or as `WKB` objects, which contain binary data in WKB
format.  Querying geographic data returns `WKB` objects and `WKB`
objects may be used in queries.  When using text representations in
queries, the text must be converted to geography data in SQL using the
`ST_GEOGFROMTEXT` SQL function, or by wrapping the value in a `WKT`
objects (the text version or `WKB` objects) in Python.

