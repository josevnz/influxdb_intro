#!/usr/bin/env python
"""
Most common 10 crime types of all time
Author: Jose Vicente Nunez (kodegeek.com@protonmail.com)
Example:
    scripts/most_common_crime_types.py --cases 20 ct_data.cfg --start=-12y
"""
from argparse import ArgumentParser
from configparser import ConfigParser
from pathlib import Path

from influxdb_client import InfluxDBClient
from rich.console import Console
from rich.table import Table
from rich.traceback import install

install(show_locals=True)
TIMEOUT_IN_MILLIS = 3_600_000


def cases_per_town(url: str, token: str, org: str, bucket: str, max_cases: int = 10, start: str = "-2y"):
    with InfluxDBClient(url=url, token=token, org=org, timeout=TIMEOUT_IN_MILLIS) as client:
        query = f"""from(bucket: "{bucket}")
    |> range(start: {start})
    |> filter(fn: (r) => r._measurement == "policeincidents" and r._field == "ucr_1_code")
    |> group(columns: ["ucr_1_description"])
    |> drop(columns: ["neighborhood", "s2_cell_id", "lon", "lat", "ucr_2_category", "ucr_2_description", "ucr_2_code", "address"])
    |> count(column: "_value")
    |> group()
    |> sort(columns: ["_value"], desc: true)
    |> limit(n: {max_cases})"""
        with Console() as console:
            tables = client.query_api().query(query, org=org)
            screen_table = Table(title=f"Most common {max_cases} crime types of all time")
            screen_table.add_column("Description", style="magenta")
            screen_table.add_column("Cases", justify="right", style="green")
            for table in tables:
                for record in table.records:
                    description = record['ucr_1_description']
                    count = f"{record['_value']:,}"
                    screen_table.add_row(description, count)
            console.print(screen_table)


if __name__ == "__main__":
    PARSER = ArgumentParser(__doc__)
    PARSER.add_argument('--start', action='store', default='-2y', help=f"Start time")
    PARSER.add_argument('--cases', action='store', type=int, default=20, help=f"Maximum number of cases")
    PARSER.add_argument('config', type=Path, help=f"Path to the configuration file")
    CFG = ConfigParser()
    ARGS = PARSER.parse_args()
    CFG.read(ARGS.config)

    ORG = CFG.get('police_cases', 'org')
    BUCKET = CFG.get('police_cases', 'bucket')
    TOKEN = CFG.get('police_cases', 'api_token')
    URL = CFG.get('police_cases', 'url')
    try:
        cases_per_town(url=URL, token=TOKEN, org=ORG, bucket=BUCKET, max_cases=ARGS.cases, start=ARGS.start)
    except KeyboardInterrupt:
        pass
