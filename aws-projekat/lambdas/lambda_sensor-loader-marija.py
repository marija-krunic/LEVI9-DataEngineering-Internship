import os
import boto3

s3 = boto3.client("s3")

SOURCE_BUCKET = os.environ["SOURCE_BUCKET"]
DEST_BUCKET = os.environ["DEST_BUCKET"]
CITY_PREFIX = os.environ["CITY_PREFIX"]           # npr. "pollution/grad/"
DEST_ROOT_PREFIX = os.environ["DEST_ROOT_PREFIX"] # npr. "pollution_archive/grad/"

# Opcioni env varovi, ako želiš da ih menjaš bez koda
MIN_DAY = int(os.environ.get("MIN_DAY", "21"))    # podrazumevano 12
MAX_DAY = int(os.environ.get("MAX_DAY", "31"))    # podrazumevano 30


def lambda_handler(event, context):
    global CITY_PREFIX

    print(f"SOURCE_BUCKET = {SOURCE_BUCKET}")
    print(f"DEST_BUCKET = {DEST_BUCKET}")
    print(f"CITY_PREFIX = '{CITY_PREFIX}'")
    print(f"DEST_ROOT_PREFIX = '{DEST_ROOT_PREFIX}'")
    print(f"MIN_DAY = {MIN_DAY}, MAX_DAY = {MAX_DAY}")

    # Normalizuj CITY_PREFIX da se završava sa '/'
    if CITY_PREFIX and not CITY_PREFIX.endswith("/"):
        CITY_PREFIX = CITY_PREFIX + "/"
        print(f"Normalizovan CITY_PREFIX = '{CITY_PREFIX}'")

    continuation_token = None
    copied = 0
    touched_folders = set()

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

            # preskoči "folder" objekte
            if source_key.endswith("/"):
                continue

            # npr. source_key = "pollution/grad/11-03-2022/something.csv"
            # CITY_PREFIX      = "pollution/grad/"
            # relative_path    = "11-03-2022/something.csv"
            if not source_key.startswith(CITY_PREFIX):
                continue  # safety

            relative_path = source_key[len(CITY_PREFIX):]

            # prvi deo je dnevni folder: "11-03-2022"
            parts = relative_path.split("/", 1)
            if not parts:
                continue

            date_folder = parts[0]  # "11-03-2022"
            touched_folders.add(date_folder)

            # izvučemo dan: "11" -> 11
            try:
                day_str = date_folder.split("-", 1)[0]  # uzmi deo pre prvog '-'
                day = int(day_str)
            except Exception as e:
                print(f"Ne mogu da izvučem dan iz foldera '{date_folder}', preskačem. Greška: {e}")
                continue

            # filtriramo: samo dani MIN_DAY..MAX_DAY
            if not (MIN_DAY <= day <= MAX_DAY):
                # npr. dan je 01–11 ili 31 -> preskačemo
                continue

            dest_key = f"{DEST_ROOT_PREFIX}{relative_path}"

            copy_source = {
                "Bucket": SOURCE_BUCKET,
                "Key": source_key
            }

            print(
                f"[{date_folder}] Kopiram s3://{SOURCE_BUCKET}/{source_key} "
                f"-> s3://{DEST_BUCKET}/{dest_key}"
            )

            try:
                s3.copy_object(
                    Bucket=DEST_BUCKET,
                    Key=dest_key,
                    CopySource=copy_source
                )
                copied += 1
            except Exception as e:
                print(f"[{date_folder}] Greška pri kopiranju {source_key}: {e}")

        if not response.get("IsTruncated"):
            break

        continuation_token = response.get("NextContinuationToken")

    print(f"Folderi (datumi) koje sam video: {sorted(touched_folders)}")
    print(f"Ukupno kopirano objekata: {copied}")
    return {
        "statusCode": 200,
        "body": f"Copied {copied} objects for days {MIN_DAY}-{MAX_DAY}"
    }
