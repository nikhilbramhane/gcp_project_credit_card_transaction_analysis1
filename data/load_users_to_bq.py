from pyspark.sql import SparkSession
from pyspark.sql.functions import input_file_name, when

# Create Spark session
spark = SparkSession.builder \
                    .appName("Cardholder Data Ingestion") \
                    .getOrCreate()

# configure variables
BUCKET_NAME = "avd-bucket1-credit-card-analysis"
CLAIMS_BUCKET_PATH = f"gs://{BUCKET_NAME}/landing/users/*.csv"
BQ_TABLE = "prefab-pursuit-468112-h4.credit_card.users"
TEMP_GCS_BUCKET = f"{BUCKET_NAME}/temp/"

# read from claims source
claims_df = spark.read.csv(CLAIMS_BUCKET_PATH, header=True)

# dropping dupplicates if any
claims_df = claims_df.dropDuplicates()

# write to bigquery
(claims_df.write
            .format("bigquery")
            .option("table", BQ_TABLE)
            .option("temporaryGcsBucket", TEMP_GCS_BUCKET)
            .mode("overwrite")
            .save())
