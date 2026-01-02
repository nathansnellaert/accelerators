
from subsets_utils import get, save_raw_json

API_URL = "https://500.co/api/startups"


def run():
    response = get(API_URL)
    data = response.json()

    if data.get("status") != 200:
        raise ValueError(f"API returned status {data.get('status')}")

    raw_companies = data.get("res", [])
    print(f"Fetched {len(raw_companies)} companies from 500 Global")

    save_raw_json(raw_companies, "global_500_companies")
