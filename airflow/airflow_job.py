from datetime import timedelta
import uuid
from airflow.utils.dates import days_ago
from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import DataprocCreateBatchOperator
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator
from airflow.utils.trigger_rule import TriggerRule

# DAG default arguments
default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}

with DAG(
    dag_id="credit_card_transactions_dataproc_dag2",
    default_args=default_args,
    schedule_interval="0 5 * * *",
) as dag:

    # GCS Config
    gcs_bucket = "avd-bucket1-credit-card-analysis"
    file_pattern = "transactions/transactions_"
    source_prefix = "transactions/"
    archive_prefix = "archive/"

    # Generate unique batch IDs
    batch_id_users = f"load-users-batch-{str(uuid.uuid4())[:8]}"
    batch_id_txns = f"credit-card-batch-{str(uuid.uuid4())[:8]}"

    #Dataproc Job 1: Load users to BigQuery
    load_users_task = DataprocCreateBatchOperator(
        task_id="load_users_to_bq",
        batch={
            "pyspark_batch": {
                "main_python_file_uri": f"gs://{gcs_bucket}/spark_job/load_users_to_bq.py"
            },
            "runtime_config": {
                "version": "2.2.61",
            },
            "environment_config": {
                "execution_config": {
                    "service_account": "630875432759-compute@developer.gserviceaccount.com",
                    "network_uri": "projects/prefab-pursuit-468112-h4/global/networks/default",
                    "subnetwork_uri": "projects/prefab-pursuit-468112-h4/regions/us-central1/subnetworks/default",
                }
            },
        },
        batch_id=batch_id_users,
        project_id="prefab-pursuit-468112-h4",
        region="us-central1",
        gcp_conn_id="google_cloud_default",
    )

    # Dataproc Job 2: Process transactions, "version": "2.2",
    process_txns_task = DataprocCreateBatchOperator(
        task_id="run_credit_card_processing_job",
        batch={
            "pyspark_batch": {
                "main_python_file_uri": f"gs://{gcs_bucket}/spark_job/spark_job.py"
            },
            "runtime_config": {
                "version": "2.2.61",
            },
            "environment_config": {
                "execution_config": {
                    "service_account": "630875432759-compute@developer.gserviceaccount.com",
                    "network_uri": "projects/prefab-pursuit-468112-h4/global/networks/default",
                    "subnetwork_uri": "projects/prefab-pursuit-468112-h4/regions/us-central1/subnetworks/default",
                }
            },
        },
        batch_id=batch_id_txns,
        project_id="prefab-pursuit-468112-h4",
        region="us-central1",
        gcp_conn_id="google_cloud_default",
    )

    # Archive processed files
    move_files_to_archive = GCSToGCSOperator(
        task_id="move_files_to_archive",
        source_bucket=gcs_bucket,
        source_object=source_prefix,
        destination_bucket=gcs_bucket,
        destination_object=archive_prefix,
        move_object=True,
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    # DAG Flow
    #load_users_task >> process_txns_task >> move_files_to_archive
    process_txns_task >> move_files_to_archive

