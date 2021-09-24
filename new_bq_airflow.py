from airflow import DAG
from airflow.utils.dates import days_ago, timedelta
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

default_args = {
    'owner' : 'cdna',
    'depends_on_past' : False,
    'start_date' : days_ago(2), #datetime(2021, 9, 10),
    'email' : ['phani.krishna@cardinalhealth.com', 'shareef.shaik@cardinalhealth.com'],
    'email_on_failure' : True,
    'email_on_retry' : True,
    'retries' : 1,
    'retry_delay' : timedelta(minutes=5)
}

tags = ['generated', 'run_query', 'Scott']

dag = DAG(
    dag_id = "cdna_bq_dag",
    description = "sample_run",
    schedule_interval = "@daily",
    default_args = default_args,
    tags = tags,
    start_date = days_ago(2), #datetime(2021, 9, 10),
)

t1_sql = Open("query_file.txt", "r")
t1_sql = t1_sql.read()
t1 = BigQueryInsertJobOperator(
    task_id = "sp_load_bq_data",
    dag = dag,
    useLegacySql = False,
    sql = t1_sql
    #gcp_conn_id =  
)

t1