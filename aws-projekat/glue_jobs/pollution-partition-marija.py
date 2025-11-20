import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql.functions import col, to_date

# ---------------------------
# Čitanje argumenata iz Glue joba
# ---------------------------
args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'SOURCE_S3',
    'TARGET_S3'
])

source_s3 = args['SOURCE_S3']   # npr. s3://tvoj-bucket/weather/
target_s3 = args['TARGET_S3']   # npr. s3://tvoj-target-bucket/partitioned/

# ---------------------------
# Inicijalizacija Glue / Spark konteksta
# ---------------------------
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

print("==== GLUE WEATHER ETL STARTED ====")
print(f"Source S3 path : {source_s3}")
print(f"Target S3 path : {target_s3}")

# ---------------------------
# Učitavanje CSV fajlova rekurzivno iz svih podfoldera
# ---------------------------
# VAŽNO: SOURCE_S3 neka bude root (npr. s3://bucket/weather/),
# a ovo će pokupiti sve fajlove u podfolderima (datumi).
df = (
    spark.read
        .option("header", "true")             # imamo header
        .option("inferSchema", "true")        # neka proba da pogodi tipove
        .option("recursiveFileLookup", "true")# uključi rekurziju po folderima
        .csv(source_s3)
)

print("=== Ulazni dataframe schema ===")
df.printSchema()

print("=== Primer ulaznih podataka ===")
df.show(20, truncate=False)

# ---------------------------
# Dodavanje kolone date iz time_date
# ---------------------------
# Pretpostavka: time_date je formata "yyyy-MM-dd HH:mm:ss"
# npr. "2022-04-01 02:31:00"
df_with_date = df.withColumn("date", to_date(col("time_date")))

print("=== Primer konverzije time_date -> date ===")
df_with_date.select("time_date", "date").show(20, truncate=False)

# (Opcionalno) možeš da izbaciš redove gde date nije uspelo da se parsira:
df_final = df_with_date.filter(col("date").isNotNull())

print("Ukupan broj redova (pre filtera):", df_with_date.count())
print("Ukupan broj redova (posle filtera date IS NOT NULL):", df_final.count())

# ---------------------------
# Upis particionisanih podataka na target S3
# ---------------------------
print("=== Upisujem particionisane podatke na target S3 ===")

(
    df_final.write
        # overwrite = pregazi ceo target; append = dodaj (koristi šta ti treba)
        .mode("append")
        .partitionBy("date")
        .parquet(target_s3)
)

print("=== Upis završen. Posao uspešno odrađen. ===")

job.commit()
