Working with Geographic data
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

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
from the `pybigquery.sqlalchemy_bigquery` module::

  from sqlalchemy.ext.declarative import declarative_base
  from sqlalchemy import Column, String
  from pybigquery.sqlalchemy_bigquery import GEOGRAPHY

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
objects may be used in queries.  When
calling spatial functions that expect geographic arguments, text
arguments are automatically coerced to geography.

Inserting data
~~~~~~~~~~~~~~

When inserting geography data, you can pass WKT strings, `WKT` objects,
or `WKB` objects::

  from sqlalchemy_bigquery import WKT

  lake  = Lake(name='Majeur', geom='POLYGON((0 0,1 0,1 1,0 1,0 0))')
  lake2 = Lake(name='Garde', geom=WKT('POLYGON((1 0,3 0,3 2,1 2,1 0))'))
  b = WKT('POLYGON((3 0,6 0,6 3,3 3,3 0))').wkb
  lake3 = Lake(name='Orta', geom=b)

Note that in the `lake3` example, we got a `WKB` object by creating a
`WKT` object and getting its `wkb` property.  Normally, we'd get `WKB`
objects as results of previous queries.

Queries
~~~~~~~

When performing spacial queries, and geography objects are expected,
you can to pass `WKB` or `WKT` objects::

  query = session.query(Lake).filter(
      func.ST_Contains(Lake.geom, some_wkb_or_wkt))

Or you can pass strings in WKT format::

  query = session.query(Lake).filter(
      func.ST_Contains(Lake.geom, 'POINT(4 1)'))

In this example, `Lake.geom` is a geography column.  The point
constant needs to be wrapped in a `WKT` object.

Installing geography support
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

To get geography support, you need to install `pybigquery`
with the `geography` extra, or separately install `GeoAlchemy2` and
`shapely`.
