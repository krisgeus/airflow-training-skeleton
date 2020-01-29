import datetime
import airflow
from airflow.operators.python_operator import ShortCircuitOperator
from airflow.contrib.operators.postgres_to_gcs_operator import (
    PostgresToGoogleCloudStorageOperator,
)
from airflow.contrib.operators.dataproc_operator import (
    DataprocClusterCreateOperator,
    DataProcPySparkOperator,
    DataprocClusterDeleteOperator,
)
from airflow.models import DAG
from operators.http_to_gcs_operator import HttpToGcsOperator
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator


project_id="airflowbolcom-jan2829-99875f84"
analytics_bucket_name="europe-west1-training-airfl-840ef3d9-bucket"
bucket_name="airflow-training-data-land-registry-krisgeus"
currency="EUR"

args = {
    "owner": "Kris",
    "start_date": datetime.datetime(2019,11,24),
}

def check_date(execution_date, **context):
    return execution_date <= datetime.datetime(2019,11,28)

with DAG(
    dag_id="usecase",
    default_args=args,
    schedule_interval="@daily"
) as dag:

    check_date = ShortCircuitOperator(
        task_id="check_if_before_end_of_last_year",
        python_callable=check_date,
        provide_context=True,
    )

    psql_to_gcs = PostgresToGoogleCloudStorageOperator(
        task_id="postgres_to_gcs",
        sql="SELECT * FROM land_registry_price_paid_uk WHERE transfer_date = '{{ ds }}'",
        bucket="airflow-training-data-land-registry-krisgeus",
        filename="usecase/land_registry_price_paid_uk/{{ ds }}/properties_{}.json",
        postgres_conn_id="airflow-training-postgres",
    )


    #https://api.exchangeratesapi.io/history?start_at=2018-01-01&end_at=2018-01-02&symbols=EUR&base=GBP
    http_to_gcs = HttpToGcsOperator(
        task_id="get_currency_" + currency,
        method="GET",
        endpoint=f"/history?start_at={{{{ ds }}}}&end_at={{{{ ds_tomorrow }}}}&base=GBP&symbols={currency}",
        http_conn_id="currency-http",
        gcs_conn_id="google_cloud_storage_default",
        gcs_path=f"usecase/currency/{{{{ ds }}}}-{currency}.json",
        gcs_bucket=f"{bucket_name}",
    )

    dataproc_create_cluster = DataprocClusterCreateOperator(
        task_id="create_dataproc",
        cluster_name="analyse-pricing-{{ ds }}",
        project_id=project_id,
        num_workers=2,
        zone="europe-west4-a",
    )

    compute_aggregates = DataProcPySparkOperator(
        task_id="compute_aggregates",
        main=f"gs://{analytics_bucket_name}/build_statistics.py",
        cluster_name="analyse-pricing-{{ ds }}",
        arguments=[
            f"gs://{bucket_name}/usecase/land_registry_price_paid_uk/*/*.json",
            f"gs://{bucket_name}/usecase/currency/{{{{ ds }}}}-{currency}.json",
            f"gs://{bucket_name}/usecase/results/{{{{ ds }}}}/",
            currency,
            "{{ ds }}"
        ],
    )

    dataproc_delete_cluster = DataprocClusterDeleteOperator(
        task_id="delete_dataproc",
        cluster_name="analyse-pricing-{{ ds }}",
        project_id=project_id,
    )

    results_to_bigquery = GoogleCloudStorageToBigQueryOperator(
        task_id='results_to_bigquery',
        bucket=bucket_name,
        source_objects=[
            'usecase/results/{{ ds }}/part-*'
        ],
        destination_project_dataset_table='airflow_dataset.results_{{ ds_nodash }}',
        source_format='PARQUET',
        create_disposition='CREATE_IF_NEEDED',
        write_disposition='WRITE_TRUNCATE',
        google_cloud_storage_conn_id='google_cloud_storage_default',
        bigquery_conn_id='bigquery_default',
        autodetect=True,
    )

check_date >> [psql_to_gcs, http_to_gcs] >> dataproc_create_cluster >> compute_aggregates >> [ dataproc_delete_cluster, results_to_bigquery ]
