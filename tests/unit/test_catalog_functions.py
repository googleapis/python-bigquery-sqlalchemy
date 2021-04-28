import pytest




@pytest.mark.parametrize(
    "table,schema,expect",
    [
        ("p.s.t", None, "p.s.t"),
        ("p.s.t", "p.s", "p.s.t"),

        # Why is a single schema name a project name when a table
        # dataset id is given?  I guess to provde a missing default.
        ("p.s.t", "p", "p.s.t"),
        ("s.t", "p", "p.s.t"),

        ("s.t", "p.s", "p.s.t"),
        ("s.t", None, "myproject.s.t"),
        ("t", None, "myproject.mydataset.t"),
        ("t", "s", "myproject.s.t"),
        ("t", "q.s", "q.s.t"),
    ]
)
def test__table_reference(faux_conn, table, schema, expect):
    assert str(
        faux_conn.dialect._table_reference(
            schema, table, faux_conn.connection._client.project
        )
    ) == expect


@pytest.mark.parametrize(
    "table,table_project,schema,schema_project",
    [
        ("p.s.t", "p", "q.s", "q"),
        ("p.s.t", "p", "q", "q"),
    ]
)
def test__table_reference_inconsistent_project(
    faux_conn, table, table_project, schema, schema_project
):
    with pytest.raises(
        ValueError,
        match=(f"project_id specified in schema and table_name disagree: "
               f"got {schema_project} in schema, and {table_project} in table_name"),
    ):
        faux_conn.dialect._table_reference(
            schema, table, faux_conn.connection._client.project
        )


@pytest.mark.parametrize(
    "table,table_dataset,schema,schema_dataset",
    [
        ("s.t", "s", "p.q", "q"),
        ("p.s.t", "s", "p.q", "q"),
    ]
)
def test__table_reference_inconsistent_dataset_id(
    faux_conn, table, table_dataset, schema, schema_dataset
):
    with pytest.raises(
        ValueError,
        match=(f"dataset_id specified in schema and table_name disagree: "
               f"got {schema_dataset} in schema, and {table_dataset} in table_name"),
    ):
        faux_conn.dialect._table_reference(
            schema, table, faux_conn.connection._client.project
        )
