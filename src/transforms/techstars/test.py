import pyarrow as pa
from subsets_utils import validate
from subsets_utils.testing import assert_max_length, assert_in_range


def test(table: pa.Table) -> None:
    """Validate Techstars companies output."""
    # 1. Schema validation - every column must be listed
    validate(table, {
        "columns": {
            "id": "string",
            "name": "string",
            "description": "string",
            "city": "string",
            "state_province": "string",
            "country": "string",
            "world_region": "string",
            "world_subregion": "string",
            "first_session_year": "int",
            "industry_verticals": "list",
            "program_names": "list",
            "program_slugs": "list",
            "is_unicorn": "bool",
            "is_bcorp": "bool",
            "has_exited": "bool",
            "is_current_session": "bool",
            "website": "string",
            "linkedin_url": "string",
            "twitter_url": "string",
            "facebook_url": "string",
            "crunchbase_url": "string",
            "logo_url": "string",
        },
        "not_null": ["id", "name"],
        "unique": ["id"],
        "min_rows": 3000,
    })

    # 2. String length validations
    assert_max_length(table, "name", 300)
    assert_max_length(table, "city", 100)
    assert_max_length(table, "country", 100)

    # 3. Year validation
    years = [v for v in table.column("first_session_year").to_pylist() if v is not None]
    assert all(2000 <= y <= 2030 for y in years), "Session years should be reasonable (2000-2030)"

    # 4. Region values should be non-empty for most records
    regions = [v for v in table.column("world_region").to_pylist() if v]
    assert len(regions) > len(table) * 0.5, "At least half of records should have a region"

    # 5. Native arrays should be lists
    industries = [v for v in table.column("industry_verticals").to_pylist() if v]
    for ind in industries[:10]:
        assert isinstance(ind, list), f"Industry verticals should be a list: {ind}"

    # 6. Reasonable number of unicorns
    unicorns = sum(1 for v in table.column("is_unicorn").to_pylist() if v)
    total = len(table)
    assert unicorns < total * 0.1, f"Unicorn ratio seems too high: {unicorns}/{total}"

    print(f"  Validated {len(table):,} companies")
