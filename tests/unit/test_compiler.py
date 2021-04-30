import sqlalchemy


def test_constraints_are_ignored(faux_conn):

    metadata = sqlalchemy.MetaData()

    table = sqlalchemy.Table(
        "ref",
        metadata,
        sqlalchemy.Column("id", sqlalchemy.Integer),
    )

    table = sqlalchemy.Table(
        "some_table",
        metadata,
        sqlalchemy.Column("id", sqlalchemy.Integer, primary_key=True),
        sqlalchemy.Column(
            "ref_id", sqlalchemy.Integer, sqlalchemy.ForeignKey("ref.id")
        ),
        sqlalchemy.UniqueConstraint('id', 'ref_id', name='uix_1'),
    )

    metadata.create_all(faux_conn.engine)

    assert ' '.join(faux_conn.test_data["execute"][-1][0].strip().split()
                    ) == ('CREATE TABLE `some_table`'
                          ' ( `id` INT64 NOT NULL, `ref_id` INT64 )'
                          )
