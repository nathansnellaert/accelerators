import pyarrow as pa
from subsets_utils import load_raw_json, upload_data, publish
from .test import test


DATASET_ID = "yc_companies"

METADATA = {
    "id": DATASET_ID,
    "title": "Y Combinator Portfolio Companies",
    "description": "Startup companies from Y Combinator accelerator batches",
    "column_descriptions": {
        "id": "Unique company identifier",
        "name": "Company name",
        "slug": "URL-friendly company identifier",
        "batch": "YC batch identifier",
        "description": "Short company description",
        "long_description": "Detailed company description",
        "website": "Company website URL",
        "status": "Company status",
        "tags": "List of category tags",
        "location": "Company headquarters location",
        "country": "Country name",
        "team_size": "Number of employees",
        "linkedin_url": "LinkedIn company page URL",
        "twitter_url": "Twitter profile URL",
        "facebook_url": "Facebook page URL",
        "crunchbase_url": "Crunchbase profile URL",
        "logo_url": "Company logo image URL",
        "is_hiring": "Whether company is actively hiring",
        "is_nonprofit": "Whether company is a nonprofit organization",
        "highlight_black": "Founder diversity highlight for Black founders",
        "highlight_latinx": "Founder diversity highlight for Latinx founders",
        "highlight_women": "Founder diversity highlight for Women founders",
    }
}


def run():
    """Transform Y Combinator companies data."""
    all_companies = load_raw_json("yc_companies")
    print(f"  Loaded {len(all_companies)} raw companies")

    processed = []
    for company in all_companies:
        if not company.get("name"):
            continue

        processed.append({
            "id": str(company.get("id", "")),
            "name": company.get("name", ""),
            "slug": company.get("slug", "") or "",
            "batch": company.get("batch", "") or "",
            "description": company.get("one_liner", "") or "",
            "long_description": company.get("long_description", "") or "",
            "website": company.get("website", "") or "",
            "status": company.get("status", "") or "",
            "tags": company.get("tags") if company.get("tags") else None,
            "location": company.get("all_locations", "") or "",
            "country": company.get("country", "") if company.get("country") else "",
            "team_size": company.get("team_size"),
            "linkedin_url": company.get("linkedin_url", "") or "",
            "twitter_url": company.get("twitter_url", "") or "",
            "facebook_url": company.get("facebook_url", "") or "",
            "crunchbase_url": company.get("cb_url", "") or "",
            "logo_url": company.get("small_logo_thumb_url", "") or "",
            "is_hiring": bool(company.get("isHiring", False)),
            "is_nonprofit": bool(company.get("nonprofit", False)),
            "highlight_black": bool(company.get("highlight_black", False)),
            "highlight_latinx": bool(company.get("highlight_latinx", False)),
            "highlight_women": bool(company.get("highlight_women", False)),
        })

    if not processed:
        raise ValueError("No YC companies found")

    # Deduplicate by id (keep last occurrence)
    seen = {}
    for company in processed:
        seen[company["id"]] = company
    processed = list(seen.values())

    print(f"  Transformed {len(processed):,} unique companies")
    table = pa.Table.from_pylist(processed)

    test(table)

    upload_data(table, DATASET_ID, mode="merge", merge_key="id")
    publish(DATASET_ID, METADATA)


if __name__ == "__main__":
    run()
