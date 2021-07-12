
def test_geoalchemy2_core(bigquery_dataset):
    """Make sure GeoAlchemy 2 Core Tutorial works as adapted to only having geometry

    https://geoalchemy-2.readthedocs.io/en/latest/core_tutorial.html

    Note:

    - BigQuery doesn't implicitly convert WKT strings to
      geography when calling geography functions that want geography
      arguments.
    - Bigquery doesn't have ST_BUFFER
    """
    from sqlalchemy import create_engine
    engine = create_engine(f'bigquery:///{bigquery_dataset}')

    from sqlalchemy import Table, Column, Integer, String, MetaData
    from sqlalchemy_bigquery import GEOGRAPHY

    metadata = MetaData()
    lake_table = Table(
        'lake',
        metadata,
        Column('name', String),
        Column('geog', GEOGRAPHY)
    )

    lake_table.create(engine)

    conn = engine.connect()

    conn.execute(lake_table.insert().values(
        name='Majeur',
        geog='POLYGON((0 0,1 0,1 1,0 1,0 0))',
        )
    )

    conn.execute(lake_table.insert(), [
        {'name': 'Garde', 'geog': 'POLYGON((1 0,3 0,3 2,1 2,1 0))'},
        {'name': 'Orta', 'geog': 'POLYGON((3 0,6 0,6 3,3 3,3 0))'}
        ])

    from sqlalchemy.sql import select

    assert sorted(
        (r.name, r.geog.desc[:4])
        for r in conn.execute(select([lake_table]))
        ) == [('Garde', '0103'), ('Majeur', '0103'), ('Orta', '0103')]

    from sqlalchemy import func
    from sqlalchemy_bigquery import WKT

    assert sorted(
        (r.name, int(r.area))
        for r in conn.execute(
            select([lake_table.c.name, lake_table.c.geog.ST_AREA().label('area')])
            )
        ) == [('Garde', 49452374328),
              ('Majeur', 12364036567),
              ('Orta', 111253664228)]
