
from subsets_utils import get, save_raw_json

TYPESENSE_CONFIG = {
    "host": "https://8gbms7c94riane0lp-1.a1.typesense.net",
    "collection": "companies",
    "api_key": "0QKFSu4mIDX9UalfCNQN4qjg2xmukDE0",
}

PER_PAGE = 250


def fetch_companies_page(page: int) -> dict:
    url = f"{TYPESENSE_CONFIG['host']}/collections/{TYPESENSE_CONFIG['collection']}/documents/search"
    params = {
        "q": "",
        "query_by": "company_name,brief_description,city,state_province,country,worldregion,program_names,industry_vertical",
        "sort_by": "website_order:asc",
        "per_page": PER_PAGE,
        "page": page,
    }
    headers = {
        "X-TYPESENSE-API-KEY": TYPESENSE_CONFIG["api_key"],
        "Accept": "application/json",
    }
    response = get(url, params=params, headers=headers)
    return response.json()


def run():
    all_companies = []
    page = 1

    first_response = fetch_companies_page(page)
    total_found = first_response["found"]
    print(f"Total companies available: {total_found}")

    all_companies.extend(first_response["hits"])
    print(f"Page {page}: fetched {len(first_response['hits'])} companies")

    while len(all_companies) < total_found:
        page += 1
        response = fetch_companies_page(page)
        hits = response["hits"]
        if not hits:
            break
        all_companies.extend(hits)
        print(f"Page {page}: fetched {len(hits)} companies (total: {len(all_companies)})")

    print(f"Total fetched: {len(all_companies)} Techstars companies")
    save_raw_json(all_companies, "techstars_companies")
