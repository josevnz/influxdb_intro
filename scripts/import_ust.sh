#!/usr/bin/env bash

:<<DOC
This script expects to have a token defined as an environment variable, like this:
export API_TOKEN="ddNjO0sMa4_r8TM60LJKRjUlzttaGCKSIADhTt9lhhHLgA4nnvNE26FgnasqhTDiczvnf5XL2nMJZNMuTgu3Vg=="
DOC

BUCKET=USTS
ORG=KodeGeek

if ! ID=$(/bin/basename "$0"); then
  echo "ERROR: Unable to figure out program name"
  exit 100
fi

if ! /usr/bin/podman pull influxdb; then
  echo "ERROR: Unable to pull latest version of influxdb"
  exit 99
fi

if [ -z "$1" ]; then
  logger --id "$ID" --stderr "Missing url parameter"
  exit 100
fi
url=$1
if [ -z "$API_TOKEN" ]; then
  logger --id "$ID" --stderr "API_TOKEN environment variable is missing"
  exit 100
fi

if [ ! -f "$2" ]; then
  logger --id "$ID" --stderr "Missing path of CSV file to import"
  exit 100
fi
csv_file="$2"

dryrun=$3
if [ -n "$dryrun" ]; then
  logger --id "$ID" --stderr "DRY-RUN detected, now data will be imported"
fi

:<<DOC
ST Site ID Number,Site Name,Site Address,Site City,Site Zip,Tank No.,Status of Tank,Compartment,Estimated Total Capacity (gallons),Substance Currently Stored,Last Used Date,Closure Type,Construction Type - Tank,Tank Details,Construction Type - Piping,Piping Details,Installation Date,Spill Protection,Overfill Protection,Tank Latitude,Tank Longitude,Tank Collection Method,Tank Reference Point Type,UST Site Latitude,UST Site Longitude,Site Collection Method,Site Reference Point Type
50-11456,Brewer Dauntless Marina,9 NOVELTY LN,ESSEX,06426,1,Permanently Closed,,4000,Gasoline,10/18/2018,Tank was Removed From Ground,Coated & Cathodically Protected Steel (sti-P3),Double Walled,Flexible Plastic,"Containment Sumps @ Dispensers,Containment Sumps @ Tanks,Double Walled,Metallic fittings isolated from soil and water",06/01/1999,Spill Bucket,Ball Float Device,41.350018,-72.385442,Address Matching,Approximate Location,41.350018,-72.385442,Address Matching,Approximate Location
DOC

# We define our own header and pass it along the data
if ! header_file=$(/bin/mktemp); then
  logger --id "$ID" --stderr "Cannot create temp file with headers"
  exit 102
fi
# shellcheck disable=SC2064
trap "/bin/rm -f $header_file" INT QUIT EXIT

# We skip the file headers to inject our own (--skipHeader=1)
# As use the CSV annotations: https://docs.influxdata.com/influxdb/v2.4/write-data/developer-tools/csv/#csv-annotations
# We need to use a custom date time format, as explained here: https://pkg.go.dev/time
/bin/cat<<HEADER>"$header_file"
#constant measurement,fuel_tanks
#datatype ignore,ignore,ignore,tag,ignore,ignore,tag,tag,long,tag,dateTime:01/02/2006,tag,tag,ignore,tag,ignore,ignore,tag,tag,double,double,ignored,ignored,ignored,ignored,ignored,ignored
ID,Name,Address,City,Zip,TankNo,Status,Compartment,EstimatedTotalCapacity,SubstanceStored,LastUsed,ClosureType,ConstructionType,Details,ConstructionType,PipingDetails,InstallationDate,SpillProtection,OverfillProtection,Latitude,Longitude,CollectionMethod,ReferencePointType,USTLatitude,USTLongitude,CollectionMethod,ReferencePointType
HEADER

if ! /usr/bin/podman run \
  --interactive \
  --tty \
  --volume "$header_file:/data/headers.csv" \
  --volume "$csv_file:/data/tanks.csv" \
  influxdb influx write "$dryrun" \
  --bucket $BUCKET \
  --org $ORG \
  --format csv \
  --skipHeader=1 \
  --url "$url" \
  --file "/data/headers.csv" \
  --file "/data/tanks.csv"; then
  logger --id "$ID" --stderr "USTS import failed!"
  exit 100
fi