import os
import json
import time
import boto3

# DynamoDB setup
dynamodb = boto3.resource("dynamodb")
table = dynamodb.Table(os.environ["DDB_TABLE_NAME"])

# Dozvoljeni root folderi
ALLOWED_PREFIXES = (
    "bronze-layer-marija-nsdata/pollution/",
    "bronze-layer-marija-nsdata/sensor/",
    "bronze-layer-marija-nsdata/weather/",
)

def lambda_handler(event, context):
    print("Lambda start, broj SQS poruka:", len(event.get("Records", [])))

    # Prolazimo kroz sve SQS poruke u batch-u
    for sqs_record in event.get("Records", []):
        body = sqs_record["body"]
        print("SQS body:", body)

        try:
            # S3 šalje event JSON kao string u body
            s3_event = json.loads(body)
        except json.JSONDecodeError as e:
            print(f"Body nije validan JSON: {e}")
            # ako ovo pukne, pusti exception da bi SQS uradio retry / DLQ
            raise

        # S3 event može imati više Records
        for record in s3_event.get("Records", []):
            s3_info = record.get("s3", {})
            bucket = s3_info.get("bucket", {}).get("name")
            key = s3_info.get("object", {}).get("key")

            if not bucket or not key:
                print("Nedostaje bucket ili key u S3 eventu, bacam grešku.")
                # namerno pravimo grešku da ode na DLQ posle par pokušaja
                raise Exception("S3 event bez bucket ili key")

            print(f"Stigao objekat: s3://{bucket}/{key}")

            # 1) Filtriranje na pollution / sensor / weather
            if not key.startswith(ALLOWED_PREFIXES):
                print(f"Preskačem {key} – nije pollution/sensor/weather")
                continue

            # 2) Priprema podataka za DynamoDB
            timestamp = int(time.time())
            status = 0  # initial status = 0

            item = {
                "filename": key,
                "timestamp": timestamp,
                "status": status
            }

            print(f"Upisujem u DynamoDB: {item}")

            # 3) Insert u DynamoDB
            try:
                table.put_item(Item=item)
            except Exception as e:
                print(f"Greška pri upisu u DynamoDB: {e}")
                # Pusti grešku da bi SQS radio retry i eventualno DLQ
                raise

    print("Lambda end.")
    return {"statusCode": 200, "body": "OK"}
