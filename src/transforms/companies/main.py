"""
Consolidated accelerator companies dataset.

Combines companies from all accelerator sources into a single dataset
with a minimal common schema.
"""

import json
import pyarrow as pa
from pathlib import Path

from subsets_utils import upload_data, load_asset

DATASET_ID = "accelerator_companies"

SOURCES = [
    ("500_global", "500_global_companies"),
    # ("plug_and_play", "plug_and_play_companies"),  # API blocks cloud IPs
    ("techstars", "techstars_companies"),
    ("ycombinator", "yc_companies"),
]

METADATA = {
    "id": DATASET_ID,
    "title": "Accelerator Companies",
    "description": "Companies from top startup accelerators (500 Global, Techstars, Y Combinator)",
}

# Load country mapping
MAPPINGS_DIR = Path(__file__).parent.parent.parent / "mappings"
COUNTRY_ISO = json.loads((MAPPINGS_DIR / "country_iso.json").read_text())


def load_source(source_id: str, table_name: str) -> list[dict]:
    """Load and normalize a source dataset."""
    df = load_asset(table_name)

    rows = []
    for i in range(len(df)):
        country = df["country"][i].as_py()
        rows.append({
            "id": f"{source_id}_{df['id'][i].as_py()}",
            "source": source_id,
            "name": df["name"][i].as_py(),
            "description": df["description"][i].as_py(),
            "website": df["website"][i].as_py(),
            "logo_url": df["logo_url"][i].as_py(),
            "country": country,
            "country_iso": COUNTRY_ISO.get(country) if country else None,
        })

    return rows


def run():
    all_rows = []

    for source_id, table_name in SOURCES:
        print(f"Loading {source_id}...")
        rows = load_source(source_id, table_name)
        print(f"  {len(rows)} companies")
        all_rows.extend(rows)

    print(f"\nTotal: {len(all_rows)} companies")

    # Create PyArrow table
    table = pa.Table.from_pylist(all_rows, schema=pa.schema([
        ("id", pa.string()),
        ("source", pa.string()),
        ("name", pa.string()),
        ("description", pa.string()),
        ("website", pa.string()),
        ("logo_url", pa.string()),
        ("country", pa.string()),
        ("country_iso", pa.string()),
    ]))

    upload_data(table, DATASET_ID, METADATA, mode="overwrite")


if __name__ == "__main__":
    run()
