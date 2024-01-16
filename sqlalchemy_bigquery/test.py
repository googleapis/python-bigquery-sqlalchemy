from sqlalchemy.sql.expression import cast
from sqlalchemy import String, func

code_coding = db.query(func.unnest(self.model.code_coding)).subquery()
conditions.append(
    and_(
        cast(code_coding.c.code, String) == "H",
        cast(code_coding.c.display, String) == "BLAST",
    )
)
