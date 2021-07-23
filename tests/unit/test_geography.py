import sys

import pytest

from conftest import (
    setup_table,
    sqlalchemy_1_4_or_higher,
)

pytestmark = pytest.mark.skipif(
    sys.version_info[:2] != (3, 9),
    reason="We install geoalchemy2 only for Python 3.9")


def test_geoalchemy2_core(faux_conn, last_query):
    """Make sure GeoAlchemy 2 Core Tutorial works as adapted to only having geometry
    """
    conn = faux_conn

    # Create the Table

    from sqlalchemy import Column, String
    from pybigquery.sqlalchemy_bigquery import GEOGRAPHY

    lake_table = setup_table(
        conn, "lake", Column("name", String), Column("geog", GEOGRAPHY)
    )

    # Insertions

    conn.execute(
        lake_table.insert().values(
            name="Majeur", geog="POLYGON((0 0,1 0,1 1,0 1,0 0))",
        )
    )

    last_query(
        'INSERT INTO `lake` (`name`, `geog`)'
        ' VALUES (%(name:STRING)s, %(geog:geography)s)',
        ({'geog': 'POLYGON((0 0,1 0,1 1,0 1,0 0))', 'name': 'Majeur'}))

    conn.execute(
        lake_table.insert(),
        [
            {"name": "Garde", "geog": "POLYGON((1 0,3 0,3 2,1 2,1 0))"},
            {"name": "Orta", "geog": "POLYGON((3 0,6 0,6 3,3 3,3 0))"},
        ],
    )
    last_query(
        'INSERT INTO `lake` (`name`, `geog`)'
        ' VALUES (%(name:STRING)s, %(geog:geography)s)',
        {'name': 'Garde', 'geog': 'POLYGON((1 0,3 0,3 2,1 2,1 0))'},
        offset=2,
        )
    last_query(
        'INSERT INTO `lake` (`name`, `geog`)'
        ' VALUES (%(name:STRING)s, %(geog:geography)s)',
        {'name': 'Orta', 'geog': 'POLYGON((3 0,6 0,6 3,3 3,3 0))'},
        )

    # Selections

    from sqlalchemy.sql import select

    try: conn.execute(select([lake_table]))
    except Exception: pass  # sqlite had no special functions :)
    last_query('SELECT `lake`.`name`, ST_AsBinary(`lake`.`geog`) AS `geog` \n'
             'FROM `lake`')

    # Spatial query

    from sqlalchemy import func

    try:
        conn.execute(
            select([lake_table.c.name],
                   func.ST_Contains(lake_table.c.geog, "POINT(4 1)"))
        )
    except Exception: pass  # sqlite had no special functions :)
    last_query(
        'SELECT `lake`.`name` \n'
        'FROM `lake` \n'
        'WHERE ST_Contains(`lake`.`geog`, %(ST_Contains_1:geography)s)',
        {'ST_Contains_1': 'POINT(4 1)'})

    try:
        conn.execute(
            select([lake_table.c.name, lake_table.c.geog.ST_AREA().label("area")])
            )
    except Exception: pass  # sqlite had no special functions :)
    last_query('SELECT `lake`.`name`, ST_Area(`lake`.`geog`) AS `area` \nFROM `lake`')

    # Extra: Make sure we can save a retrieved value back:

    from pybigquery.sqlalchemy_bigquery import WKB, WKT
    geog = WKT('point(0 0)').wkb
    assert isinstance(geog, WKB)
    assert geog.data == (
        b'\x01\x01\x00\x00\x00\x00\x00\x00\x00\x00'
        b'\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00')
    conn.execute(lake_table.insert().values(name="test", geog=geog))
    last_query('INSERT INTO `lake` (`name`, `geog`)'
             ' VALUES (%(name:STRING)s, %(geog:geography)s)',
             {'name': 'test', 'geog': 'POINT (0 0)'})

    # and, while we're at it, that we can insert WKTs, although we
    # normally wouldn't want to.

    conn.execute(
        lake_table.insert().values(
            name="test2",
            geog=WKT("POLYGON((1 0,3 0,3 2,1 2,1 0))"),
        )
    )
    last_query(
        'INSERT INTO `lake` (`name`, `geog`)'
        ' VALUES (%(name:STRING)s, %(geog:geography)s)',
        {'name': 'test2', 'geog': 'POLYGON((1 0,3 0,3 2,1 2,1 0))'},
    )


def test_WKB_bad_srid():
    from pybigquery.sqlalchemy_bigquery import WKB

    with pytest.raises(AssertionError, match="Bad srid"):
        WKB('data', srid=-1)


def test_WKB_bad_extended():
    from pybigquery.sqlalchemy_bigquery import WKB

    with pytest.raises(AssertionError, match="Extended must be True."):
        WKB('data', extended=False)


def test_calling_st_functions_we_that_dont_take_geographies(faux_conn, last_query):
    from sqlalchemy import select, func
    try: faux_conn.execute(select([func.ST_GEOGFROMTEXT("point(0 0)")]))
    except Exception: pass  # sqlite had no special functions :)

    last_query(
        'SELECT ST_AsBinary(ST_GeogFromText(%(ST_GeogFromText_2:STRING)s))'
        ' AS `ST_GeogFromText_1`',
        dict(ST_GeogFromText_2="point(0 0)"),
    )
