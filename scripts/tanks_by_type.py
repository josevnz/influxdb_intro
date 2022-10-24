#!/usr/bin/env python
"""
Number of tanks, grouped by substance type
Author: Jose Vicente Nunez (kodegeek.com@protonmail.com)
"""
from argparse import ArgumentParser
from configparser import ConfigParser
from pathlib import Path

from influxdb_client import InfluxDBClient
from rich.console import Console
from rich.table import Table

TIMEOUT_IN_MILLIS = 600000


def tanks_per_type(url: str, token: str, org: str, bucket: str, start: str = "-3y"):
    with InfluxDBClient(url=url, token=token, org=org, timeout=TIMEOUT_IN_MILLIS) as client:
        query = f'''from(bucket: "{bucket}")
    |> range(start: {start})
    |> filter(fn: (r) => r._measurement == "fuel_tanks" and r._field == "estimated_total_capacity" and r.status == "currently in use")
    |> group(columns: ["substance_stored"])
    |> count(column: "_value")
    |> drop(
        columns: [
            "city",
            "closure_type",
            "construction_type",
            "overfill_protection",
            "s2_cell_id",
            "lat",
            "lon",
            "_time",
            "spill_protection",
            "status",
        ],
    )
    |> group()
    |> sort(columns: ["_value"], desc: true)'''
        with Console() as console:
            tables = client.query_api().query(query, org=org)
            screen_table = Table(title=f"Number of tanks, grouped by substance type (start: {start}")
            screen_table.add_column("Substance", justify="right", style="cyan", no_wrap=True)
            screen_table.add_column("Count", justify="right", style="green")
            for table in tables:
                for record in table.records:  # Should be always 1 table, last group() takes care of that
                    substance = record['substance_stored']
                    count = f"{record['_value']:,}"
                    screen_table.add_row(substance, count)
            console.print(screen_table)


if __name__ == "__main__":
    PARSER = ArgumentParser(__doc__)
    PARSER.add_argument('--start', action='store', default='-15y', help=f"Start time")
    PARSER.add_argument('config', type=Path, help=f"Path to the configuration file")
    CFG = ConfigParser()
    ARGS = PARSER.parse_args()
    CFG.read(ARGS.config)

    ORG = CFG.get('usts', 'org')
    BUCKET = CFG.get('usts', 'bucket')
    TOKEN = CFG.get('usts', 'api_token')
    URL = CFG.get('usts', 'url')

    tanks_per_type(url=URL, token=TOKEN, org=ORG, bucket=BUCKET, start=ARGS.start)
