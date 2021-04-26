import sqlalchemy


def test_labels_not_forced(faux_conn):
    metadata = sqlalchemy.MetaData()
    table = sqlalchemy.Table(
        "some_table", metadata, sqlalchemy.Column("id", sqlalchemy.Integer)
    )
    metadata.create_all(faux_conn.engine)
    result = faux_conn.execute(sqlalchemy.select([table.c.id]))
    assert result.keys() == ["id"]  # Look! Just the column name!


def test_typed_parameters(faux_conn):
    metadata = sqlalchemy.MetaData()
    table = sqlalchemy.Table(
        "some_table", metadata, sqlalchemy.Column("id", sqlalchemy.Integer)
    )
    metadata.create_all(faux_conn.engine)
    select = sqlalchemy.select([table.c.id]).where(table.c.id == 42)
    faux_conn.execute(select)
    assert faux_conn.test_data["execute"][1] == (
        "SELECT `some_table`.`id` \n"
        "FROM `some_table` \n"
        "WHERE `some_table`.`id` = %(id_1:INT64)s",
        {"id_1": 42},
    )
