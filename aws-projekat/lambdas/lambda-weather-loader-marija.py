import os
import boto3

s3 = boto3.client("s3")

SOURCE_BUCKET = os.environ["SOURCE_BUCKET"]
DEST_BUCKET = os.environ["DEST_BUCKET"]
CITY_PREFIX = os.environ["CITY_PREFIX"]           # npr. "pollution/grad/"
DEST_ROOT_PREFIX = os.environ["DEST_ROOT_PREFIX"] # npr. "pollution_archive/grad/"

def lambda_handler(event, context):
    print(f"SOURCE_BUCKET = {SOURCE_BUCKET}")
    print(f"DEST_BUCKET = {DEST_BUCKET}")
    print(f"CITY_PREFIX = {CITY_PREFIX}")
    print(f"DEST_ROOT_PREFIX = {DEST_ROOT_PREFIX}")

    continuation_token = None
    copied = 0

    while True:
        list_kwargs = {
            "Bucket": SOURCE_BUCKET,
            "Prefix": CITY_PREFIX,
            "MaxKeys": 200,
        }
        if continuation_token:
            list_kwargs["ContinuationToken"] = continuation_token

        response = s3.list_objects_v2(**list_kwargs)
        contents = response.get("Contents", [])
        print(f"Našao objekata u ovom batch-u: {len(contents)}")

        if not contents:
            break

        for obj in contents:
            source_key = obj["Key"]

            if source_key.endswith("/"):
                continue

            relative_path = source_key[len(CITY_PREFIX):]
            dest_key = f"{DEST_ROOT_PREFIX}{relative_path}"

            copy_source = {
                "Bucket": SOURCE_BUCKET,
                "Key": source_key
            }

            print(f"Kopiram s3://{SOURCE_BUCKET}/{source_key} -> s3://{DEST_BUCKET}/{dest_key}")

            try:
                s3.copy_object(
                    Bucket=DEST_BUCKET,
                    Key=dest_key,
                    CopySource=copy_source
                )
                copied += 1
            except Exception as e:
                print(f"Greška pri kopiranju {source_key}: {e}")

        if not response.get("IsTruncated"):
            break

        continuation_token = response.get("NextContinuationToken")

    print(f"Ukupno kopirano objekata: {copied}")
    return {
        "statusCode": 200,
        "body": f"Copied {copied} objects"
    }
