import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql.functions import col, to_date, lit

# ---------------------------
# Čitanje argumenata iz Glue joba
# ---------------------------
args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'SOURCE_S3',
    'TARGET_S3'
])

# Pobrisi slučajne space-ove sa početka/kraja
source_s3 = args['SOURCE_S3'].strip()   # npr. s3://marija-demo-bucket-v1/sensor/
target_s3 = args['TARGET_S3'].strip()   # npr. s3://marija-demo-bucket-v1/sensor_partitioned/

# ---------------------------
# Init Glue / Spark
# ---------------------------
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

print("==== GLUE SENSOR ETL STARTED ====")
print(f"Source S3 path : {source_s3}")
print(f"Target S3 path : {target_s3}")

# ---------------------------
# Učitavanje CSV fajlova rekurzivno iz svih podfoldera
# ---------------------------
df = (
    spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .option("recursiveFileLookup", "true")
        .csv(source_s3)
)

print("=== Ulazni dataframe schema ===")
df.printSchema()

print("=== Primer ulaznih podataka ===")
df.show(20, truncate=False)

# ---------------------------
# Pretvori time_nano (epoch nanos) u timestamp i date
# ---------------------------
# time_nano npr. 1648771203930224648
#  -> / 1_000_000_000 = sekunde od epohe
#  -> cast u timestamp
df_ts = df.withColumn(
    "timestamp",
    (col("time_nano") / lit(1_000_000_000)).cast("timestamp")
)

df_with_date = df_ts.withColumn("date", to_date(col("timestamp")))

print("=== Primer konverzije time_nano -> timestamp -> date ===")
df_with_date.select("time_nano", "timestamp", "date").show(20, truncate=False)

# ---------------------------
# Filtriraj validne datume (za svaki slučaj)
# ---------------------------
df_final = df_with_date.filter(col("date").isNotNull())

print("Ukupan broj redova sa validnim date:", df_final.count())

# ---------------------------
# Upis na S3, particionisano po date
# ---------------------------
print("=== Upisujem particionisane sensor podatke na target S3 ===")

(
    df_final.write
        .mode("append")          # ili 'overwrite' za prvi, inicijalni run
        .partitionBy("date")
        .parquet(target_s3)
)

print("=== Sensor ETL uspešno završen. ===")
job.commit()
