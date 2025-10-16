import sys
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, when, lit, to_timestamp, concat, round as spark_round
)

# Filter Valid Transactions
def filter_valid_transactions(df: DataFrame) -> DataFrame:
    return df.filter(
        (col("transaction_amount") > 0) &
        (col("transaction_status").isin("SUCCESS", "FAILED", "PENDING")) &
        col("cardholder_id").isNotNull() &
        col("merchant_id").isNotNull()
    )

# Enrich Transaction Columns
def enrich_transactions(df: DataFrame) -> DataFrame:
    return df.select("*").withColumns({
        "transaction_category": when(col("transaction_amount") <= 100, lit("Low"))
            .when(col("transaction_amount") <= 500, lit("Medium"))
            .otherwise(lit("High")),
        "transaction_timestamp": to_timestamp(col("transaction_timestamp")),
        "high_risk": (col("fraud_flag") == True) |
                     (col("transaction_amount") > 10000) |
                     (col("transaction_category") == "High"),
        "merchant_info": concat(col("merchant_name"), lit(" - "), col("merchant_location"))
    })

# Add Risk and Reward Columns
def add_risk_and_rewards(df: DataFrame) -> DataFrame:
    return df.withColumns({
        "updated_reward_points": col("reward_points") + spark_round(col("transaction_amount") / 10),
        "fraud_risk_level": when(col("high_risk"), lit("Critical"))
            .when((col("risk_score") > 0.3) | (col("fraud_flag")), lit("High"))
            .otherwise(lit("Low"))
    })

# Full Processing Pipeline
def process_transactions(trans_df: DataFrame, cardholders_df: DataFrame) -> DataFrame:
    df = filter_valid_transactions(trans_df)
    df = enrich_transactions(df)
    df = df.join(cardholders_df, on="cardholder_id", how="left")
    df = add_risk_and_rewards(df)
    return df

# Main Execution
if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("Credit Card Transactions Processor") \
        .getOrCreate()

    # GCP Config
    PROJECT_ID = "prefab-pursuit-468112-h4"
    GCS_BUCKET_NAME = "avd-bucket-credit-card-analysis"
    BQ_DATASET = "credit_card"
    json_file_path = f"gs://{GCS_BUCKET_NAME}/landing/transactions/transactions_*.json"
    BQ_CARDHOLDERS_TABLE = f"{PROJECT_ID}.{BQ_DATASET}.users"
    BQ_TRANSACTIONS_TABLE = f"{PROJECT_ID}.{BQ_DATASET}.transactions"
    TEMP_GCS_BUCKET = f"{GCS_BUCKET_NAME}/temp/"

    # Load Data
    cardholders_df = spark.read.format("bigquery") \
        .option("table", BQ_CARDHOLDERS_TABLE) \
        .load()

    transactions_df = spark.read.option("multiline", "true").json(json_file_path)

    # Transform
    enriched_df = process_transactions(transactions_df, cardholders_df)

    # Write to BigQuery
    enriched_df.write.format("bigquery") \
        .option("table", BQ_TRANSACTIONS_TABLE) \
        .option("temporaryGcsBucket", TEMP_GCS_BUCKET) \
        .option("createDisposition", "CREATE_IF_NEEDED") \
        .option("writeDisposition", "WRITE_APPEND") \
        .save()

    spark.stop()