import pyarrow as pa
from subsets_utils import load_raw_json, upload_data, publish
from .test import test


DATASET_ID = "plug_and_play_companies"

METADATA = {
    "id": DATASET_ID,
    "title": "Plug and Play Portfolio Companies",
    "description": "Startup companies in the Plug and Play Tech Center accelerator portfolio",
    "column_descriptions": {
        "id": "Unique company identifier",
        "name": "Company name",
        "description": "Detailed company description",
        "one_liner": "Brief one-line description",
        "website": "Company website URL",
        "logo_url": "Company logo image URL",
        "country": "Country where company is based",
        "location": "City or location name",
        "main_industry": "Primary industry vertical",
        "industries": "List of all industry verticals",
        "is_unicorn": "Whether company has reached unicorn status",
        "has_exited": "Whether company has had an exit",
        "is_accelerated": "Whether company went through accelerator program",
    }
}


def run():
    """Transform Plug and Play companies data."""
    all_companies = load_raw_json("plug_and_play_companies")
    print(f"  Loaded {len(all_companies)} raw companies")

    processed = []
    for company in all_companies:
        title = company.get("startupTitle", "")
        if not title:
            continue

        country_info = company.get("startupCountry")
        country = country_info.get("countryName", "") if isinstance(country_info, dict) else (country_info or "")

        location_info = company.get("startupLocation")
        location = location_info.get("locationName", "") if isinstance(location_info, dict) else (location_info or "")

        industry_info = company.get("startupMainIndustry")
        main_industry = industry_info.get("industryTitle", "") if isinstance(industry_info, dict) else (industry_info or "")

        industries_list = company.get("startupIndustriesList", {})
        industry_names = []
        if isinstance(industries_list, dict):
            for key, val in industries_list.items():
                if isinstance(val, dict) and "startupIndustry" in val:
                    ind = val["startupIndustry"]
                    if isinstance(ind, dict) and ind.get("industryTitle"):
                        industry_names.append(ind["industryTitle"])

        processed.append({
            "id": company.get("startupPlaybookID", "") or company.get("@id", ""),
            "name": title,
            "description": company.get("startupDescription", "") or "",
            "one_liner": company.get("startupOneLiner", "") or "",
            "website": company.get("startupWebsite", "") or "",
            "logo_url": company.get("startupLogo", "") or "",
            "country": country,
            "location": location,
            "main_industry": main_industry,
            "industries": industry_names if industry_names else None,
            "is_unicorn": bool(company.get("startupUnicorn", False)),
            "has_exited": bool(company.get("startupExit", False)),
            "is_accelerated": bool(company.get("startupAccelerated", False)),
        })

    if not processed:
        raise ValueError("No Plug and Play companies found")

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
