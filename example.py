import airflow
from airflow import DAG
from lib.prestodag import PrestoDAG

p = PrestoDAG(
    dag_id='business_transaction_events_mapping',
    yaml_file='/files/dags/yaml/business_transaction_events_mapping-Presto-etl.yaml',
)
dag = p.run_dag()
