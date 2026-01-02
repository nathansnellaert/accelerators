import pyarrow as pa
from subsets_utils import validate
from subsets_utils.testing import assert_max_length, assert_min_length


def test(table: pa.Table) -> None:
    """Validate 500 Global companies output."""
    # 1. Schema validation - every column must be listed
    validate(table, {
        "columns": {
            "id": "string",
            "name": "string",
            "legal_name": "string",
            "description": "string",
            "website": "string",
            "linkedin_url": "string",
            "logo_url": "string",
            "country": "string",
            "region": "string",
            "stage": "string",
            "business_model": "string",
            "industries": "list",
            "batches": "list",
            "programs": "list",
            "first_investment_date": "string",
        },
        "not_null": ["id", "name"],
        "unique": ["id"],
        "min_rows": 1000,
    })

    # 2. ID format validation
    ids = [v for v in table.column("id").to_pylist() if v]
    assert all(id.isdigit() for id in ids), "IDs should be numeric strings"

    # 3. String length validations
    assert_max_length(table, "name", 200)
    assert_max_length(table, "country", 100)

    # 4. URL format checks - most websites should have a domain
    websites = [v for v in table.column("website").to_pylist() if v]
    websites_with_domain = [w for w in websites if "." in w]
    assert len(websites_with_domain) / len(websites) > 0.8, "Most websites should contain a domain"

    # 5. Date format check for first_investment_date
    dates = [v for v in table.column("first_investment_date").to_pylist() if v]
    for d in dates[:10]:
        assert "T" in d or len(d) == 10, f"Unexpected date format: {d}"

    # 6. Native arrays should be lists
    industries = [v for v in table.column("industries").to_pylist() if v]
    for ind in industries[:10]:
        assert isinstance(ind, list), f"Industries should be a list: {ind}"

    print(f"  Validated {len(table):,} companies")
