import geoalchemy2
import sqlalchemy.sql.type_api

SRID = 4326  # WGS84


class WKB(geoalchemy2.WKBElement):

    geom_from_extended_version = 'ST_GeogFromWKB'

    def __init__(self, data, srid=SRID, extended=True):
        if srid != SRID:
            raise AssertionError("Bad srid", srid)
        if not extended:
            raise AssertionError("Extended must be True.")
        super().__init__(data, srid, True)

    @property
    def wkt(self):
        return WKT(wkt.dumps(wkb.loads(self.data)))


class WKT(geoalchemy2.WKTElement):

    geom_from_extended_version = 'ST_GeogFromText'

    def __init__(self, data):
        super().__init__(data, SRID, True)

    @property
    def wkb(self):
        return WKB(wkb.dumps(wkt.loads(self.data)))


class GEOGRAPHY(geoalchemy2.Geography):

    ElementType = WKB

    def __init__(self):
        super().__init__(
            geometry_type=None,
            spatial_index=False,
            srid=SRID,
        )
        self.extended = True

    # Un-inherit the bind function that adds an ST_GeogFromText.
    # It's unnecessary and causes BigQuery to error.
    bind_expression = sqlalchemy.sql.type_api.TypeEngine.bind_expression
