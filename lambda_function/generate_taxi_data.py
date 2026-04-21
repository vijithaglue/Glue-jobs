"""Lambda function to generate sample taxi lookup data.

Writes ~42 MB of CSV data per invocation (hourly), partitioned by
date and hour to s3://athena-ctas-result/SampleTaxis/.

Partition layout:
  SampleTaxis/year=YYYY/month=MM/day=DD/hour=HH/data.csv
"""

import boto3
import csv
import io
import random
import string
from datetime import datetime, timezone

S3_BUCKET = "athena-ctas-result"
S3_PREFIX = "SampleTaxis"
TARGET_BYTES = 42 * 1024 * 1024  # ~42 MB per hour

BOROUGHS = ["Manhattan", "Brooklyn", "Queens", "Bronx", "Staten Island", "EWR"]
ZONES = [
    "Newark Airport", "Jamaica Bay", "Allerton/Pelham Gardens",
    "Alphabet City", "Arden Heights", "Arrochar/Fort Wadsworth",
    "Astoria", "Astoria Park", "Auburndale", "Baisley Park",
    "Bath Beach", "Battery Park", "Battery Park City", "Bay Ridge",
    "Bay Terrace/Fort Totten", "Bayside", "Bedford", "Bedford Park",
    "Bellerose", "Belmont", "Bensonhurst East", "Bensonhurst West",
    "Bloomfield/Emerson Hill", "Bloomingdale", "Boerum Hill",
    "Borough Park", "Breezy Point/Fort Tilden", "Briarwood/Jamaica Hills",
    "Brighton Beach", "Broad Channel", "Bronx Park", "Bronxdale",
    "Brooklyn Heights", "Brooklyn Navy Yard", "Brownsville",
    "Bushwick North", "Bushwick South", "Cambria Heights",
    "Canarsie", "Carroll Gardens", "Central Harlem", "Central Harlem North",
    "Central Park", "Charleston/Tottenville", "Chinatown",
    "City Island", "Claremont/Bathgate", "Clinton East", "Clinton Hill",
    "Clinton West", "Co-Op City", "Cobble Hill", "College Point",
    "Columbia Street", "Coney Island", "Corona", "Country Club",
    "Crotona Park", "Crotona Park East", "Crown Heights North",
    "Crown Heights South", "Cypress Hills", "DUMBO/Vinegar Hill",
    "Douglas Manor/Douglaston", "Downtown Brooklyn/MetroTech",
    "Dyker Heights", "East Chelsea", "East Concourse/Concourse Village",
    "East Elmhurst", "East Flatbush/Farragut", "East Flatbush/Remsen Village",
    "East Flushing", "East Harlem North", "East Harlem South",
    "East New York", "East New York/Pennsylvania Avenue",
    "East Tremont", "East Village", "East Williamsburg",
    "Eastchester", "Elmhurst", "Elmhurst/Maspeth"
]
SERVICE_ZONES = ["Boro Zone", "Yellow Zone", "Airports"]


def generate_row(location_id):
    """Generate a single taxi zone lookup row."""
    return {
        "LocationID": location_id,
        "Borough": random.choice(BOROUGHS),
        "Zone": random.choice(ZONES),
        "service_zone": random.choice(SERVICE_ZONES),
        "pickup_count": random.randint(0, 50000),
        "dropoff_count": random.randint(0, 50000),
        "avg_fare": round(random.uniform(5.0, 120.0), 2),
        "avg_distance": round(random.uniform(0.5, 30.0), 2),
        "avg_duration_min": round(random.uniform(2.0, 90.0), 1),
        "avg_tip": round(random.uniform(0.0, 25.0), 2),
        "avg_total": round(random.uniform(6.0, 150.0), 2),
        "trip_type": random.choice(["Street-hail", "Dispatch"]),
        "payment_type": random.choice(["Credit card", "Cash", "No charge", "Dispute"]),
        "notes": ''.join(random.choices(string.ascii_letters + " ", k=random.randint(20, 100)))
    }


def handler(event, context):
    """Lambda handler. Generates ~42 MB CSV and writes to S3."""
    now = datetime.now(timezone.utc)
    partition_path = (
        f"{S3_PREFIX}/year={now.year}/month={now.month:02d}/"
        f"day={now.day:02d}/hour={now.hour:02d}/data.csv"
    )

    buf = io.StringIO()
    writer = None
    location_id = 1
    bytes_written = 0

    while bytes_written < TARGET_BYTES:
        row = generate_row(location_id)
        if writer is None:
            writer = csv.DictWriter(buf, fieldnames=row.keys())
            writer.writeheader()
        writer.writerow(row)
        location_id += 1
        bytes_written = buf.tell()

    csv_bytes = buf.getvalue().encode("utf-8")
    buf.close()

    s3 = boto3.client("s3")
    s3.put_object(
        Bucket=S3_BUCKET,
        Key=partition_path,
        Body=csv_bytes,
        ContentType="text/csv"
    )

    return {
        "statusCode": 200,
        "partition": partition_path,
        "size_mb": round(len(csv_bytes) / (1024 * 1024), 2),
        "rows": location_id - 1
    }
