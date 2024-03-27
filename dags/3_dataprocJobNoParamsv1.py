import datetime

from airflow import models
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocSubmitSparkSqlJobOperator
)
from airflow.utils.dates import days_ago

# Change these values as per your project
project_id = "gcp24-409805"
region = "us-central1"
bucket_name = "usecasesrinib241"
cluster_name = "sample"

default_args = {
    # setting start date as one day ago, with that it will trigger DAG as soon as you upload it
    "project_id": project_id,
    "region": region,
    "cluster_name": cluster_name,
    "start_date": days_ago(1)
}

with models.DAG(
    "Usecase3_Job_WithNoParams",
    default_args=default_args,
    schedule_interval=datetime.timedelta(days=1)
) as dag:

    drop = DataprocSubmitSparkSqlJobOperator(
        task_id='drop',
        query_uri=f'gs://{bucket_name}/scripts/drop.sql',
    )

    customer_lo = DataprocSubmitSparkSqlJobOperator(
        task_id='customer_lo',
        query_uri=f'gs://{bucket_name}/scripts/customer_lo.sql',
    )
    
    customer_ny = DataprocSubmitSparkSqlJobOperator(
        task_id='customer_ny',
        query_uri=f'gs://{bucket_name}/scripts/customer_ny.sql',
    )
    
    salesman_lo = DataprocSubmitSparkSqlJobOperator(
        task_id='salesman_lo',
        query_uri=f'gs://{bucket_name}/scripts/salesman_lo.sql',
    )
    
    salesman_ny = DataprocSubmitSparkSqlJobOperator(
        task_id='salesman_ny',
        query_uri=f'gs://{bucket_name}/scripts/salesman_ny.sql',
    )
    
    orders = DataprocSubmitSparkSqlJobOperator(
        task_id='orders',
        query_uri=f'gs://{bucket_name}/scripts/orders.sql',
    )
 
    results_summary = DataprocSubmitSparkSqlJobOperator(
        task_id='results_summary',
        query_uri=f'gs://{bucket_name}/scripts/result_summary.sql',
    ) 
    
    drop >> customer_lo
    drop >> customer_ny
    drop >> salesman_lo
    drop >> salesman_ny
    drop >> orders
    [customer_lo,customer_ny,salesman_lo,salesman_ny,orders] >> results_summary
