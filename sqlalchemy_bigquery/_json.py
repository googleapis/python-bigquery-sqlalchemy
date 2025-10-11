import sqlalchemy


class JSON(sqlalchemy.sql.sqltypes.JSON):
    def bind_expression(self, bindvalue):
        # JSON query parameters have type STRING
        # This hook ensures that the rendered expression has type JSON
        return sqlalchemy.func.PARSE_JSON(bindvalue, type_=self)
