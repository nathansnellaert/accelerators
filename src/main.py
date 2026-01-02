import argparse
import os


from subsets_utils import validate_environment

from ingest import global_500 as ingest_global_500
from ingest import plug_and_play as ingest_plug_and_play
from ingest import techstars as ingest_techstars
from ingest import ycombinator as ingest_ycombinator

from transforms.global_500 import main as transform_global_500
from transforms.plug_and_play import main as transform_plug_and_play
from transforms.techstars import main as transform_techstars
from transforms.ycombinator import main as transform_ycombinator
from transforms.companies import main as transform_companies


ACCELERATORS = [
    ("500_global", ingest_global_500, transform_global_500),
    # ("plug_and_play", ingest_plug_and_play, transform_plug_and_play),  # API blocks cloud IPs
    ("techstars", ingest_techstars, transform_techstars),
    ("ycombinator", ingest_ycombinator, transform_ycombinator),
]

# Consolidated datasets (run after individual transforms)
CONSOLIDATED = [
    ("companies", transform_companies),
]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--ingest-only", action="store_true", help="Only fetch data from APIs")
    parser.add_argument("--transform-only", action="store_true", help="Only transform existing raw data")
    args = parser.parse_args()

    validate_environment()

    should_ingest = not args.transform_only
    should_transform = not args.ingest_only

    if should_ingest:
        print("\n" + "=" * 50)
        print("Phase 1: Ingest")
        print("=" * 50)
        for name, ingest_module, _ in ACCELERATORS:
            print(f"\n--- Ingesting {name} ---")
            ingest_module.run()

    if should_transform:
        print("\n" + "=" * 50)
        print("Phase 2: Transform")
        print("=" * 50)
        for name, _, transform_module in ACCELERATORS:
            print(f"\n--- Transforming {name} ---")
            transform_module.run()

        print("\n" + "=" * 50)
        print("Phase 3: Consolidated Datasets")
        print("=" * 50)
        for name, transform_module in CONSOLIDATED:
            print(f"\n--- Building {name} ---")
            transform_module.run()

    print("\n" + "=" * 50)
    print("All accelerators processed!")
    print("=" * 50)


if __name__ == "__main__":
    main()
