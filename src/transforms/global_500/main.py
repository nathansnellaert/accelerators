import pyarrow as pa
from subsets_utils import load_raw_json, upload_data, publish
from .test import test


DATASET_ID = "500_global_companies"

METADATA = {
    "id": DATASET_ID,
    "title": "500 Global Portfolio Companies",
    "description": "Startup companies from 500 Global accelerator programs",
    "column_descriptions": {
        "id": "Unique company identifier",
        "name": "Company brand name",
        "legal_name": "Legal entity name",
        "description": "Company description",
        "website": "Company website URL",
        "linkedin_url": "LinkedIn URL",
        "logo_url": "Logo URL",
        "country": "Country of operation",
        "region": "Geographic region",
        "stage": "Funding stage",
        "business_model": "Business model type",
        "industries": "Industry verticals",
        "batches": "500 Global batch names",
        "programs": "Program slugs",
        "first_investment_date": "Date of first investment",
    }
}


def run():
    """Transform 500 Global companies data."""
    raw_companies = load_raw_json("global_500_companies")
    print(f"  Loaded {len(raw_companies)} raw companies")

    processed = []
    for company in raw_companies:
        org = company.get("organization", {})
        if not org.get("name") and not org.get("businessName"):
            continue

        investments = company.get("investments", [])
        first_investment_date = investments[0].get("initialInvestDate") if investments else None

        batches = company.get("batches", [])
        batch_names = [b.get("brandName") for b in batches if b.get("brandName")]

        industries = company.get("industries", [])
        industry_names = [i.get("name") for i in industries if i.get("name")]

        tenants = company.get("tenant", [])
        program_slugs = [t.get("slug") for t in tenants if t.get("slug")]

        country_info = org.get("countryOfOperation", {})
        country = country_info.get("name", "") if country_info else ""

        region_info = org.get("regionOfOperation", {})
        region = region_info.get("name", "") if region_info else ""

        processed.append({
            "id": str(company.get("id", "")),
            "name": org.get("businessName") or org.get("name", ""),
            "legal_name": org.get("name", ""),
            "description": company.get("oneLiner", "") or "",
            "website": org.get("companyUrl", "") or "",
            "linkedin_url": org.get("companyLinkedIn", "") or "",
            "logo_url": org.get("imageUrl", "") or "",
            "country": country,
            "region": region,
            "stage": company.get("stage", {}).get("name", "") if company.get("stage") else "",
            "business_model": company.get("businessModel", {}).get("name", "") if company.get("businessModel") else "",
            "industries": industry_names if industry_names else None,
            "batches": batch_names if batch_names else None,
            "programs": program_slugs if program_slugs else None,
            "first_investment_date": first_investment_date,
        })

    if not processed:
        raise ValueError("No 500 Global companies found")

    print(f"  Transformed {len(processed):,} companies")
    table = pa.Table.from_pylist(processed)

    test(table)

    upload_data(table, DATASET_ID, mode="merge", merge_key="id")
    publish(DATASET_ID, METADATA)


if __name__ == "__main__":
    run()
