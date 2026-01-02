
from subsets_utils import get, save_raw_json

API_URL = "https://public.dxp.playbook.vc/.rest/delivery/startups/v1"
PAGE_SIZE = 100


def run():
    all_companies = []
    offset = 0

    while True:
        response = get(API_URL, params={"limit": PAGE_SIZE, "offset": offset})
        data = response.json()

        results = data.get("results", [])
        if not results:
            break

        all_companies.extend(results)
        total = data.get("total", 0)
        print(f"Fetched {len(all_companies)}/{total} companies...")

        if len(all_companies) >= total:
            break

        offset += PAGE_SIZE

    print(f"Total fetched: {len(all_companies)} Plug and Play companies")
    save_raw_json(all_companies, "plug_and_play_companies")
