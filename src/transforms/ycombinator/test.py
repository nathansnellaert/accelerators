import pyarrow as pa
from subsets_utils import validate
from subsets_utils.testing import assert_max_length, assert_in_set


def test(table: pa.Table) -> None:
    """Validate Y Combinator companies output."""
    # 1. Schema validation - every column must be listed
    validate(table, {
        "columns": {
            "id": "string",
            "name": "string",
            "slug": "string",
            "batch": "string",
            "description": "string",
            "long_description": "string",
            "website": "string",
            "status": "string",
            "tags": "list",
            "location": "string",
            "country": "string",
            "team_size": "int",
            "linkedin_url": "string",
            "twitter_url": "string",
            "facebook_url": "string",
            "crunchbase_url": "string",
            "logo_url": "string",
            "is_hiring": "bool",
            "is_nonprofit": "bool",
            "highlight_black": "bool",
            "highlight_latinx": "bool",
            "highlight_women": "bool",
        },
        "not_null": ["id", "name"],
        "unique": ["id"],
        "min_rows": 50,
    })

    # 2. String length validations
    assert_max_length(table, "name", 200)
    assert_max_length(table, "batch", 50)
    assert_max_length(table, "country", 100)

    # 3. Batch format validation
    batches = set(v for v in table.column("batch").to_pylist() if v)
    valid_prefixes = ("Summer", "Winter", "Fall", "Spring", "IK12", "Unspecified")
    for b in batches:
        assert any(b.startswith(p) for p in valid_prefixes), f"Unexpected batch format: {b}"

    # 4. Status validation
    statuses = set(v for v in table.column("status").to_pylist() if v)
    valid_statuses = {"Active", "Inactive", "Acquired", "Public", ""}
    assert statuses.issubset(valid_statuses), f"Unexpected statuses: {statuses - valid_statuses}"

    # 5. Team size should be reasonable
    team_sizes = [v for v in table.column("team_size").to_pylist() if v is not None]
    assert all(0 <= t <= 50000 for t in team_sizes), "Team sizes should be reasonable (0-50000)"

    # 6. Native arrays should be lists
    tags = [v for v in table.column("tags").to_pylist() if v]
    for t in tags[:10]:
        assert isinstance(t, list), f"Tags should be a list: {t}"

    # 7. Verify batch format
    for b in batches:
        parts = b.split()
        assert len(parts) >= 1, f"Batch should have at least one part: {b}"

    print(f"  Validated {len(table):,} companies")
