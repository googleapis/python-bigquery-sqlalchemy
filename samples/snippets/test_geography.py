

def test_geography(engine):
    from . import geography

    lakes_touching_lake2, lakes_containing = geography.example(engine)

    assert sorted(lake.name for lake in lakes_touching_lake2) == ['Majeur', 'Orta']
    assert [lake.name for lake in lakes_containing] == ['Orta']
