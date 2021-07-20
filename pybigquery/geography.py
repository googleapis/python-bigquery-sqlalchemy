from geoalchemy2 import Geography, WKTElement

SRID = 4326  # WGS84

def WKT(value):
    element = WKTElement(value, SRID, True)
    element.geom_from_extended_version = 'ST_GeogFromText'
    return element
