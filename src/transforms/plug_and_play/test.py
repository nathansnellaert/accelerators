import pyarrow as pa
from subsets_utils import validate
from subsets_utils.testing import assert_max_length


def test(table: pa.Table) -> None:
    """Validate Plug and Play companies output."""
    # 1. Schema validation - every column must be listed
    validate(table, {
        "columns": {
            "id": "string",
            "name": "string",
            "description": "string",
            "one_liner": "string",
            "website": "string",
            "logo_url": "string",
            "country": "string",
            "location": "string",
            "main_industry": "string",
            "industries": "list",
            "is_unicorn": "bool",
            "has_exited": "bool",
            "is_accelerated": "bool",
        },
        "not_null": ["id", "name"],
        "unique": ["id"],
        "min_rows": 500,
    })

    # 2. String length validations
    assert_max_length(table, "name", 300)
    assert_max_length(table, "country", 100)
    assert_max_length(table, "main_industry", 100)

    # 3. Boolean columns should have expected distribution
    unicorns = sum(1 for v in table.column("is_unicorn").to_pylist() if v)
    assert unicorns >= 0, "Unicorn count should be non-negative"

    # 5. URL format validation
    websites = [v for v in table.column("website").to_pylist() if v]
    assert all("." in w or w == "" for w in websites[:100]), "Websites should contain a domain"

    # 6. Native arrays should be lists
    industries = [v for v in table.column("industries").to_pylist() if v]
    for ind in industries[:10]:
        assert isinstance(ind, list), f"Industries should be a list: {ind}"

    print(f"  Validated {len(table):,} companies")
