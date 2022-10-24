#!/usr/bin/env python
"""
Import the Underground Storage Tanks (USTs) - Facility and Tank Details
API: https://docs.influxdata.com/influxdb/v2.4/api-guide/client-libraries/python/
Geolocation helper: https://github.com/aaliddell/s2cell
DATASET: https://data.ct.gov/api/views/utni-rddb/rows.csv?accessType=DOWNLOAD
Author: Jose Vicente Nunez (kodegeek.com@protonmail.com)

We are expecting to handle 27 columns:

| Number | Column Name                        | Type        | Remarks                            |
|--------|------------------------------------|-------------|------------------------------------|
| 1      | UST Site ID Number                 | Plain Text  | Ignoring                           |
| 2      | Site Name                          | Plain Text  | Ignoring                           |
| 3      | Site Address                       | Plain Text  | Ignoring, do not care about street |
| 4      | Site City                          | Plain Text  ||
| 5      | Site Zip                           | Plain Text  | Ignoring                           |
| 6      | Tank No.                           | Plain Text  | Ignoring                           |
| 7      | Status of Tank                     | Plain Text  ||
| 8      | Compartment                        | Plain Text  ||
| 9      | Estimated Total Capacity (gallons) | Number      ||
| 10     | Substance Currently Stored         | Plain Text  ||
| 11     | Last Used Date                     | Date & Time ||
| 12     | Closure Type                       | Plain Text  ||
| 13     | Construction Type - Tank           | Plain Text  ||
| 14     | Tank Details                       | Plain Text  | Ignoring                           |
| 15     | Construction Type - Piping         | Plain Text  ||
| 16     | Piping Details                     | Plain Text  | Ignoring                           |
| 17     | Installation Date                  | Date & Time | Ignoring                           |
| 18     | Spill Protection                   | Plain Text  ||
| 19     | Overfill Protection                | Plain Text  ||
| 20     | Tank Latitude                      | Number      ||
| 21     | Tank Longitude                     | Number      ||
| 22     | Tank Collection Method             | Plain Text  | Ignoring                           |
| 23     | Tank Reference Point Type          | Plain Text  | Ignoring                           |
| 24     | UST Site Latitude                  | Number      | Ignoring, redundant                |
| 25     | UST Site Longitude                 | Number      | Ignoring, redundant                |
| 26     | Site Collection Method             | Plain Text  | Ignoring                           |
| 27     | Site Reference Point Type          | Plain Text  | Ignoring, redundant                |

Will use S2 level 10 for our conversion:
https://learn.microsoft.com/en-us/azure/data-explorer/kusto/query/geo-point-to-s2cell-function

"""
import re
from argparse import ArgumentParser
from configparser import ConfigParser
from dataclasses import dataclass
from datetime import datetime
from enum import unique, IntEnum
from pathlib import Path
from csv import reader
from itertools import (takewhile, repeat)
from typing import List

from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import WriteOptions, WriteType
from rich.console import Console
from rich.traceback import install
from rich.progress import Progress, TextColumn, BarColumn, TaskProgressColumn, TimeRemainingColumn
import s2cell
from uszipcode import SearchEngine, SimpleZipcode

install(show_locals=True)

START_OF_TIME = "1970-01-01T00:00:00Z"
START_OF_TIME_DATE = datetime.fromisoformat("1970-01-01")
TIMEOUT_IN_MILLIS = 600000

LARGE_SYNCHRONOUS_BATCH = write_options = WriteOptions(
    batch_size=50_000,
    flush_interval=10_000,
    write_type=WriteType.synchronous
)
S2_LEVEL = 10


def count_lines(filename):
    with open(filename, 'rb') as the_file:
        buffer_gen = takewhile(lambda x: x, (the_file.read(1024 * 1024) for _ in repeat(None)))
        return sum(buf.count(b'\n') for buf in buffer_gen if buf)


def import_data(url: str, token: str, org: str, bucket: str, data_file: Path, truncate: bool = True):
    with Console() as console:
        measurement = "fuel_tanks"

        total_lines = count_lines(data_file)
        console.print(f"[green]Tank details read:[/green] {total_lines}")
        sr = SearchEngine()

        # Data point with only the data we care about (11 attributes of 27)
        @dataclass
        class TankPoint:
            city: str
            closure_type: str
            construction_type: str
            overfill_protection: str
            spill_protection: str
            status: str
            substance_stored: str
            estimated_total_capacity: int
            s2_cell_id_token: str
            lat: float
            lon: float
            last_used_date: datetime

        # Make it easier to find the 27 tokens on the CSV file
        @unique
        class UstToken(IntEnum):
            ID = 0
            NAME = 1
            ADDRESS = 2
            CITY = 3
            ZIP = 4
            TANK_NO = 5
            STATUS = 6
            COMPARTMENT = 7
            ESTIMATED_TOTAL_CAPACITY = 8
            SUBSTANCE_STORED = 9
            LAST_USED_DATE = 10
            CLOSURE_TYPE = 11
            CONSTRUCTION_TYPE_TANK = 12
            DETAILS = 13
            CONSTRUCTION_TYPE_PIPING = 14
            PIPING_DETAILS = 15
            INSTALLATION_DATE = 16
            SPILL_PROTECTION = 17
            OVERFILL_PROTECTION = 18
            LATITUDE = 19
            LONGITUDE = 20
            COLLECTION_METHOD = 21
            REFERENCE_POINT_TYPE = 22
            UST_LATITUDE = 23
            UST_LONGITUDE = 24
            COLLECTION_METHOD_SITE = 25
            REFERENCE_POINT_TYPE_SITE = 26

        tanks: List[TankPoint] = []
        with open(data_file, 'r') as data:
            csv_reader = reader(data)
            with InfluxDBClient(url=url, token=token, org=org, timeout=TIMEOUT_IN_MILLIS) as client:
                if truncate:
                    now = datetime.utcnow()
                    delete_api = client.delete_api()
                    delete_api.delete(start=START_OF_TIME, stop=f"{now.isoformat('T')}Z", bucket=bucket, org=org,
                                      predicate=f'_measurement="{measurement}"')
                write_api = client.write_api(write_options=LARGE_SYNCHRONOUS_BATCH)
                count = 0
                ignored = 0
                with Progress(TextColumn("[progress.description]{task.description}"),
                              BarColumn(),
                              TaskProgressColumn(),
                              TimeRemainingColumn()) as progress:

                    parsing_task = progress.add_task(f"[red]Parsing[/red] (total={total_lines:,} rows)...",
                                                     total=total_lines)
                    for row in csv_reader:
                        try:
                            if row[UstToken.ID] == "UST Site ID Number":
                                continue
                            task_description = f"Ignored due incomplete data: {row}"
                            """
                            LAST_USED_DATE = 05/10/2021 -> 2021-10-05 00:00:00
                            Also that date may be missing, time to apply heuristics
                            """
                            if not row[UstToken.LAST_USED_DATE] or row[UstToken.LAST_USED_DATE] == '':
                                if re.search('In Use', row[UstToken.STATUS]):
                                    last_used = datetime.now()
                                elif row[UstToken.INSTALLATION_DATE]:
                                    last_used = datetime.strptime(f"{row[UstToken.INSTALLATION_DATE]}", "%m/%d/%Y")
                                else:
                                    last_used = START_OF_TIME_DATE
                            else:
                                last_used = datetime.strptime(f"{row[UstToken.LAST_USED_DATE]}", "%m/%d/%Y")
                            city = row[UstToken.CITY].strip()
                            closure_type = row[UstToken.CLOSURE_TYPE].strip()
                            construction_type = row[UstToken.CONSTRUCTION_TYPE_PIPING].strip()
                            estimated_total_capacity = int(row[UstToken.ESTIMATED_TOTAL_CAPACITY].strip())
                            spill_protection = row[UstToken.SPILL_PROTECTION].strip()
                            overfill_protection = row[UstToken.OVERFILL_PROTECTION].strip()
                            substance_stored = row[UstToken.SUBSTANCE_STORED].strip()
                            status = row[UstToken.STATUS].strip()

                            """
                            https://docs.influxdata.com/flux/v0.x/stdlib/experimental/geo/
                            Tanks that were removed do not longer have their lat and lon available, use zip code for
                            lookup. If zipcode is missing, skip the data point, pure garbage.                                                        
                            """

                            lat = None
                            lon = None
                            if row[UstToken.LATITUDE] and row[UstToken.LONGITUDE]:
                                lat = float(row[UstToken.LATITUDE])
                                lon = float(row[UstToken.LONGITUDE])
                            elif row[UstToken.ZIP]:
                                sr_data: SimpleZipcode = sr.by_zipcode(row[UstToken.ZIP])
                                if sr_data:
                                    lat = sr_data.lat
                                    lon = sr_data.lng
                            else:
                                task_description = f"Ignored due missing lat/lon: {row}"
                            if lat and lon:
                                s2_cell_id_token = s2cell.lat_lon_to_token(lat, lon, S2_LEVEL)
                                tank: TankPoint = TankPoint(
                                    city=city,
                                    last_used_date=last_used,
                                    closure_type=closure_type,
                                    construction_type=construction_type,
                                    estimated_total_capacity=estimated_total_capacity,
                                    spill_protection=spill_protection,
                                    overfill_protection=overfill_protection,
                                    substance_stored=substance_stored,
                                    lat=lat,
                                    lon=lon,
                                    s2_cell_id_token=s2_cell_id_token,
                                    status=status
                                )
                                tanks.append(tank)
                                task_description = f"Parsed tank city={tank.city.ljust(15)}, "
                                f"status={tank.status.ljust(15)}"
                            else:
                                ignored += 1
                            progress.update(
                                parsing_task, advance=1,
                                description=task_description
                            )
                        except ValueError as ve:
                            console.print(f"ERROR: Cannot process {row}, error: {ve}.")
                            raise

                    if not tanks:
                        raise ValueError("Not a single row was parsed. Aborting!")
                    sorting_task = progress.add_task(f"[red]Sorting[/red] (total={total_lines:,} rows)...", total=1)
                    tanks.sort(key=lambda p: p.last_used_date, reverse=True)
                    progress.update(sorting_task, advance=1, description=f"Fully sorted ({len(tanks)} rows)")

                    insert_task = progress.add_task(f"[red]Inserting[/red] (total={total_lines:,} rows)...", total=total_lines)
                    for tank in tanks:
                        tank_point = Point(measurement) \
                            .tag("city", tank.city) \
                            .tag("closure_type", tank.closure_type) \
                            .tag("construction_type", tank.construction_type) \
                            .field("estimated_total_capacity", tank.estimated_total_capacity) \
                            .tag("spill_protection", tank.spill_protection) \
                            .tag("overfill_protection", tank.overfill_protection) \
                            .tag("substance_stored", tank.substance_stored) \
                            .tag("s2_cell_id", tank.s2_cell_id_token) \
                            .field("lat", tank.lat) \
                            .field("lon", tank.lon) \
                            .time(tank.last_used_date, WritePrecision.S)
                        write_api.write(bucket, org, tank_point)
                        progress.update(
                            insert_task,
                            advance=1,
                            description=f"Inserted tank from city={tank.city.ljust(15)}, status={tank.status.ljust(15)}")
                        count += 1
                    write_api.flush()
            console.print(f"[green]Imported[/green] {count} records, [red]ignored[/red] {ignored} records ...")
            sr.close()


if __name__ == "__main__":
    PARSER = ArgumentParser(__doc__)
    PARSER.add_argument('--data_file', action='store', required=True, type=Path, help=f"File with the code")
    PARSER.add_argument('config', type=Path, help=f"Path to the configuration file")
    CFG = ConfigParser()
    ARGS = PARSER.parse_args()
    CFG.read(ARGS.config)

    ORG = CFG.get('usts', 'org')
    BUCKET = CFG.get('usts', 'bucket')
    TOKEN = CFG.get('usts', 'api_token')
    URL = CFG.get('usts', 'url')

    try:
        import_data(url=URL, token=TOKEN, org=ORG, bucket=BUCKET, data_file=ARGS.data_file)
    except KeyboardInterrupt:
        pass
