import os
import boto3

s3 = boto3.client("s3")

INPUT_BUCKET = os.environ.get("INPUT_BUCKET")
OUTPUT_BUCKET = os.environ.get("OUTPUT_BUCKET")
OUTPUT_PREFIX = os.environ.get("OUTPUT_PREFIX", "processed/")

def lambda_handler(event, context):
    # Expecting S3 event
    record = event["Records"][0]
    src_bucket = record["s3"]["bucket"]["name"]
    src_key = record["s3"]["object"]["key"]

    print(f"Reading: s3://{src_bucket}/{src_key}")

    obj = s3.get_object(Bucket=src_bucket, Key=src_key)
    data = obj["Body"].read()

    new_key = f"{OUTPUT_PREFIX}{os.path.basename(src_key)}"
    print(f"Writing to: s3://{OUTPUT_BUCKET}/{new_key}")

    s3.put_object(
        Bucket=OUTPUT_BUCKET,
        Key=new_key,
        Body=data,
        ContentType="text/csv"
    )

    return {"status": "done", "dest": new_key}
