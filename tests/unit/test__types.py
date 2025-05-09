import pytest
from google.cloud.bigquery.schema import SchemaField

from sqlalchemy_bigquery._types import _get_transitive_schema_fields, STRUCT_FIELD_TYPES


def create_fut(name, field_type, mode="NULLABLE", sub_fields=None):
    """
    Helper function to create a SchemaField object for testing.
    `sub_fields` should be a list of already created SchemaField objects.
    """
    api_repr = {
        "name": name,
        "type": field_type,
        "mode": mode,
        "fields": [sf.to_api_repr() for sf in sub_fields] if sub_fields else [],
    }
    return SchemaField.from_api_repr(api_repr)


test_cases = [
    (
        "STRUCT field, not REPEATED, with sub-fields, should recurse",
        [
            create_fut(
                "s1",
                "STRUCT",
                "NULLABLE",
                sub_fields=[create_fut("child1", "STRING", "NULLABLE")],
            )
        ],
        ["s1", "s1.child1"],
    ),
    (
        "RECORD field (alias for STRUCT), not REPEATED, with sub-fields, should recurse",
        [
            create_fut(
                "r1",
                "RECORD",
                "NULLABLE",
                sub_fields=[create_fut("child_r1", "INTEGER", "NULLABLE")],
            )
        ],
        ["r1", "r1.child_r1"],
    ),
    (
        "STRUCT field, REPEATED, with sub-fields, should NOT recurse",
        [
            create_fut(
                "s2",
                "STRUCT",
                "REPEATED",
                sub_fields=[create_fut("child2", "STRING", "NULLABLE")],
            )
        ],
        ["s2"],
    ),
    (
        "Non-STRUCT field (STRING), not REPEATED, should NOT recurse",
        [create_fut("f1", "STRING", "NULLABLE")],
        ["f1"],
    ),
    (
        "Non-STRUCT field (INTEGER), REPEATED, should NOT recurse",
        [create_fut("f2", "INTEGER", "REPEATED")],
        ["f2"],
    ),
    (
        "Deeply nested STRUCT, not REPEATED, should recurse fully",
        [
            create_fut(
                "s_outer",
                "STRUCT",
                "NULLABLE",
                sub_fields=[
                    create_fut(
                        "s_inner1",
                        "STRUCT",
                        "NULLABLE",
                        sub_fields=[create_fut("s_leaf1", "STRING", "NULLABLE")],
                    ),
                    create_fut("s_sibling", "INTEGER", "NULLABLE"),
                    create_fut(
                        "s_inner2_repeated_struct",
                        "STRUCT",
                        "REPEATED",
                        sub_fields=[
                            create_fut(
                                "s_leaf2_ignored", "BOOLEAN", "NULLABLE"
                            )  # This sub-field should be ignored
                        ],
                    ),
                ],
            )
        ],
        [
            "s_outer",
            "s_outer.s_inner1",
            "s_outer.s_inner1.s_leaf1",
            "s_outer.s_sibling",
            "s_outer.s_inner2_repeated_struct",
        ],
    ),
    (
        "STRUCT field, not REPEATED, but no sub-fields, should not error and not recurse further",
        [create_fut("s3", "STRUCT", "NULLABLE", sub_fields=[])],
        ["s3"],
    ),
    (
        "Multiple top-level fields with mixed conditions",
        [
            create_fut("id", "INTEGER", "REQUIRED"),
            create_fut(
                "user_profile",
                "STRUCT",
                "NULLABLE",
                sub_fields=[
                    create_fut("name", "STRING", "NULLABLE"),
                    create_fut(
                        "addresses",
                        "RECORD",
                        "REPEATED",
                        sub_fields=[  # addresses is REPEATED STRUCT
                            create_fut(
                                "street", "STRING", "NULLABLE"
                            ),  # This sub-field should be ignored
                            create_fut(
                                "city", "STRING", "NULLABLE"
                            ),  # This sub-field should be ignored
                        ],
                    ),
                ],
            ),
            create_fut("tags", "STRING", "REPEATED"),
        ],
        ["id", "user_profile", "user_profile.name", "user_profile.addresses", "tags"],
    ),
    (
        "Empty input list of fields",
        [],
        [],
    ),
    (
        "Field type not in STRUCT_FIELD_TYPES and mode is REPEATED",
        [create_fut("f_arr", "FLOAT", "REPEATED")],
        ["f_arr"],
    ),
    (
        "Field type not in STRUCT_FIELD_TYPES and mode is not REPEATED",
        [create_fut("f_single", "DATE", "NULLABLE")],
        ["f_single"],
    ),
]


@pytest.mark.parametrize(
    "description, input_fields_list, expected_field_names", test_cases
)
def test_get_transitive_schema_fields_conditions(
    description, input_fields_list, expected_field_names
):
    """
    Tests the _get_transitive_schema_fields function, focusing on the conditional logic
    `if field.field_type in STRUCT_FIELD_TYPES and field.mode != "REPEATED":`.
    """
    result_fields = _get_transitive_schema_fields(input_fields_list)
    result_names = [field.name for field in result_fields]
    assert result_names == expected_field_names, description
