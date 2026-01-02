import pyarrow as pa
from subsets_utils import load_raw_json, upload_data, publish
from .test import test


DATASET_ID = "techstars_companies"

METADATA = {
    "id": DATASET_ID,
    "title": "Techstars Portfolio Companies",
    "description": "Startup companies from Techstars accelerator programs worldwide",
    "column_descriptions": {
        "id": "Unique company identifier",
        "name": "Company name",
        "description": "Brief company description",
        "city": "City where company is headquartered",
        "state_province": "State or province",
        "country": "Country name",
        "world_region": "Major world region",
        "world_subregion": "Subregion within world region",
        "first_session_year": "Year of first Techstars program participation",
        "industry_verticals": "List of industry vertical categories",
        "program_names": "List of Techstars program names attended",
        "program_slugs": "List of program identifier slugs",
        "is_unicorn": "Whether company has reached unicorn status",
        "is_bcorp": "Whether company is a certified B Corporation",
        "has_exited": "Whether company has had an exit",
        "is_current_session": "Whether company is in current Techstars session",
        "website": "Company website URL",
        "linkedin_url": "LinkedIn company page URL",
        "twitter_url": "Twitter profile URL",
        "facebook_url": "Facebook page URL",
        "crunchbase_url": "Crunchbase profile URL",
        "logo_url": "Company logo image URL",
    }
}


def run():
    """Transform Techstars companies data."""
    all_companies = load_raw_json("techstars_companies")
    print(f"  Loaded {len(all_companies)} raw companies")

    processed = []
    for hit in all_companies:
        doc = hit.get("document", {})
        if not doc.get("company_name"):
            continue

        processed.append({
            "id": doc.get("company_id") or doc.get("id", ""),
            "name": doc.get("company_name", ""),
            "description": doc.get("brief_description", "") or "",
            "city": doc.get("city", "") or "",
            "state_province": doc.get("state_province", "") or "",
            "country": doc.get("country", "") or "",
            "world_region": doc.get("worldregion", "") or "",
            "world_subregion": doc.get("worldsubregion", "") or "",
            "first_session_year": doc.get("first_session_year"),
            "industry_verticals": doc.get("industry_vertical") if doc.get("industry_vertical") else None,
            "program_names": doc.get("program_names") if doc.get("program_names") else None,
            "program_slugs": doc.get("program_slugs") if doc.get("program_slugs") else None,
            "is_unicorn": bool(doc.get("is_1b", False)),
            "is_bcorp": bool(doc.get("is_bcorp", False)),
            "has_exited": bool(doc.get("is_exit", False)),
            "is_current_session": bool(doc.get("is_current_session", False)),
            "website": doc.get("website", "") or "",
            "linkedin_url": doc.get("linkedin_url", "") or "",
            "twitter_url": doc.get("twitter_url", "") or "",
            "facebook_url": doc.get("facebook_url", "") or "",
            "crunchbase_url": doc.get("crunchbase_url", "") or "",
            "logo_url": doc.get("logo_url", "") or "",
        })

    if not processed:
        raise ValueError("No Techstars companies found")

    print(f"  Transformed {len(processed):,} companies")
    table = pa.Table.from_pylist(processed)

    test(table)

    upload_data(table, DATASET_ID, mode="merge", merge_key="id")
    publish(DATASET_ID, METADATA)


if __name__ == "__main__":
    run()
