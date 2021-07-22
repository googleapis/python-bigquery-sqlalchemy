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

from geoalchemy2 import WKTElement
import geoalchemy2.functions
import sqlalchemy.ext.compiler
from sqlalchemy.sql.elements import BindParameter

SRID = 4326  # WGS84


def WKT(value):
    element = WKTElement(value, SRID, True)
    element.geom_from_extended_version = "ST_GeogFromText"
    return element


@sqlalchemy.ext.compiler.compiles(geoalchemy2.functions.GenericFunction, "bigquery")
def _fixup_st_arguments(element, compiler, **kw):
    argument_types = _argument_types.get(element.name.lower())
    if argument_types:
        for argument_type, argument in zip(argument_types, element.clauses.clauses):
            if isinstance(argument, BindParameter) and (
                argument.type is not argument_type
                or not isinstance(argument.type, argument_type)
            ):
                argument.type = argument_type()

    return compiler.visit_function(element, **kw)


_argument_types = dict(
    st_area=(geoalchemy2.Geography,),
    st_asbinary=(geoalchemy2.Geography,),
    st_asgeojson=(geoalchemy2.Geography,),
    st_astext=(geoalchemy2.Geography,),
    st_boundary=(geoalchemy2.Geography,),
    st_centroid=(geoalchemy2.Geography,),
    st_centroid_agg=(geoalchemy2.Geography,),
    st_closestpoint=(geoalchemy2.Geography, geoalchemy2.Geography,),
    st_clusterdbscan=(geoalchemy2.Geography,),
    st_contains=(geoalchemy2.Geography, geoalchemy2.Geography,),
    st_convexhull=(geoalchemy2.Geography,),
    st_coveredby=(geoalchemy2.Geography, geoalchemy2.Geography,),
    st_covers=(geoalchemy2.Geography, geoalchemy2.Geography,),
    st_difference=(geoalchemy2.Geography, geoalchemy2.Geography,),
    st_dimension=(geoalchemy2.Geography,),
    st_disjoint=(geoalchemy2.Geography, geoalchemy2.Geography,),
    st_distance=(geoalchemy2.Geography, geoalchemy2.Geography,),
    st_dump=(geoalchemy2.Geography,),
    st_dwithin=(geoalchemy2.Geography, geoalchemy2.Geography,),
    st_endpoint=(geoalchemy2.Geography,),
    st_equals=(geoalchemy2.Geography, geoalchemy2.Geography,),
    st_exteriorring=(geoalchemy2.Geography,),
    st_geohash=(geoalchemy2.Geography,),
    st_intersection=(geoalchemy2.Geography, geoalchemy2.Geography,),
    st_intersects=(geoalchemy2.Geography, geoalchemy2.Geography,),
    st_intersectsbox=(geoalchemy2.Geography,),
    st_iscollection=(geoalchemy2.Geography,),
    st_isempty=(geoalchemy2.Geography,),
    st_length=(geoalchemy2.Geography,),
    st_makeline=(geoalchemy2.Geography, geoalchemy2.Geography,),
    st_makepolygon=(geoalchemy2.Geography, geoalchemy2.Geography,),
    st_makepolygonoriented=(geoalchemy2.Geography,),
    st_maxdistance=(geoalchemy2.Geography, geoalchemy2.Geography,),
    st_npoints=(geoalchemy2.Geography,),
    st_numpoints=(geoalchemy2.Geography,),
    st_perimeter=(geoalchemy2.Geography,),
    st_pointn=(geoalchemy2.Geography,),
    st_simplify=(geoalchemy2.Geography,),
    st_snaptogrid=(geoalchemy2.Geography,),
    st_startpoint=(geoalchemy2.Geography,),
    st_touches=(geoalchemy2.Geography, geoalchemy2.Geography,),
    st_union=(geoalchemy2.Geography, geoalchemy2.Geography,),
    st_union_agg=(geoalchemy2.Geography,),
    st_within=(geoalchemy2.Geography, geoalchemy2.Geography,),
    st_x=(geoalchemy2.Geography,),
    st_y=(geoalchemy2.Geography,),
)
