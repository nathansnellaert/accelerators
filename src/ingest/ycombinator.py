
import json
from subsets_utils import post, save_raw_json

ALGOLIA_CONFIG = {
    "url": "https://45bwzj1sgc-dsn.algolia.net/1/indexes/*/queries",
    "app_id": "45BWZJ1SGC",
    "api_key": "Zjk5ZmFjMzg2NmQxNTA0NGM5OGNiNWY4MzQ0NDUyNTg0MDZjMzdmMWY1NTU2YzZkZGVmYjg1ZGZjMGJlYjhkN3Jlc3RyaWN0SW5kaWNlcz1ZQ0NvbXBhbnlfcHJvZHVjdGlvbiZ0YWdGaWx0ZXJzPSU1QiUyMnljZGNfcHVibGljJTIyJTVEJmFuYWx5dGljc1RhZ3M9JTVCJTIyeWNkYyUyMiU1RA%3D%3D",
    "index_name": "YCCompany_production"
}

BATCH_VALUES = [
    'Fall 2024', 'Spring 2025',
    'Summer 2005', 'Summer 2006', 'Summer 2007', 'Summer 2008', 'Summer 2009',
    'Summer 2010', 'Summer 2011', 'Summer 2012', 'Summer 2013', 'Summer 2014',
    'Summer 2015', 'Summer 2016', 'Summer 2017', 'Summer 2018', 'Summer 2019',
    'Summer 2020', 'Summer 2021', 'Summer 2022', 'Summer 2023', 'Summer 2024',
    'Winter 2007', 'Winter 2008', 'Winter 2009', 'Winter 2010', 'Winter 2011',
    'Winter 2012', 'Winter 2013', 'Winter 2014', 'Winter 2015', 'Winter 2016',
    'Winter 2017', 'Winter 2018', 'Winter 2019', 'Winter 2020', 'Winter 2021',
    'Winter 2022', 'Winter 2023', 'Winter 2024', 'Winter 2025',
    'Unspecified', 'IK12'
]


def retrieve_batch(batch):
    body = {"requests": [{"indexName": ALGOLIA_CONFIG["index_name"], "params": f'filters=batch:"{batch}"&hitsPerPage=1000'}]}
    headers = {"accept": "application/json", "content-type": "application/x-www-form-urlencoded"}
    url = f'{ALGOLIA_CONFIG["url"]}?x-algolia-agent=Algolia%20for%20JavaScript%20(3.35.1)%3B%20Browser%3B%20JS%20Helper%20(3.11.3)&x-algolia-application-id={ALGOLIA_CONFIG["app_id"]}&x-algolia-api-key={ALGOLIA_CONFIG["api_key"]}'

    response = post(url, headers=headers, data=json.dumps(body))
    if response.status_code == 200:
        return response.json()['results'][0]['hits']
    return []


def run():
    all_companies = []
    for batch in sorted(BATCH_VALUES):
        companies = retrieve_batch(batch)
        print(f'Retrieved {len(companies)} companies for batch "{batch}"')
        all_companies.extend(companies)

    print(f"Total fetched: {len(all_companies)} YC companies")
    save_raw_json(all_companies, "yc_companies")
