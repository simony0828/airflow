import airflow
from airflow import DAG
from airflow.providers.presto.hooks.presto import PrestoHook
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator

from lib.airflowdag import AirflowDAG
from lib.prestoetlquery import PrestoETLQuery
from lib.prestodetector import PrestoDetector

from datetime import datetime

class PrestoDAG(AirflowDAG):
    def __init__(self, dag_id, yaml_file, description=""):
        super().__init__(dag_id, yaml_file, description)
        # Temporary/intermediate schema to be used in Presto
        self.tmp_schema = 'dps_presto_etl_poc_dm_csi_pii'
        # Add catalog to table names
        if 'catalog' not in self.config_data['database']:
            raise Exception('Missing catalog for Presto database type')
        self.database_catalog = self.config_data['database']['catalog']
        self.target_table = "{c}.{t}".format(c=self.database_catalog, t=self.target_table)
        for staging_table in self.staging_tables:
            table = self.staging_tables[staging_table]['table']
            table = "{c}.{t}".format(c=self.database_catalog, t=table)
            self.staging_tables[staging_table]['table'] = table

        if 'database' not in self.config_data:
            raise Exception("Missing database!")
        if 'catalog' not in self.config_data['database']:
            raise Exception("Missing catalog!")
        self.database_catalog = self.config_data['database']['catalog']

        # External class file contains all ETL queries
        self.pq = PrestoETLQuery()
        # External class file to run Detector
        self.pd = PrestoDetector(self.dq_data)
        # Airflow operator
        self.ph = PrestoHook()

    def __run_watcher(self, **kwargs):
        ''' Function to wait for upstream tables '''
        super().run_watcher()

    def __run_presto(self, **kwargs):
        ''' Function to execute query in Presto '''
        self.ph.run(kwargs['query'])

    def __run_detector(self, **kwargs):
        ''' Function to run DQ in Presto '''
        super().run_detector(self.pd)

    def __gen_tasks(self, dag):
        ''' Generate tasks to run in Airflow '''
        list_of_tasks = []

        # For Watcher
        list_of_tasks.append(PythonOperator(
            task_id='watcher',
            python_callable=self.__run_watcher,
            op_kwargs={},
            dag=dag,
        ))

        # For ETL
        for etl_name in self.etl:
            if etl_name.startswith("sh_"):
                list_of_tasks.append(BashOperator(
                    task_id=etl_name,
                    bash_command=self.etl[etl_name]['bash_commmand'],
                    dag=dag,
                ))
            else:
                if etl_name.startswith("tmp_"):
                    # Change the tmp table name for Presto
                    etl_sql = self.etl[etl_name]['sql']
                    unique_key = datetime.now().strftime('%Y%m%d%H%M%S')
                    self.tmp_tables[etl_name] = {}
                    self.tmp_tables[etl_name]['table'] = "{c}.{s}.{t}__{k}".format(
                        c=self.database_catalog,
                        s=self.tmp_schema,
                        t=etl_name,
                        k=unique_key
                    )
                    # Re-create the CREATE TABLE statement using the new tmp table name
                    self.etl[etl_name]['query'] = self.pq.gen_etl_tmp(
                        self.tmp_tables[etl_name]['table'], etl_sql)
                elif etl_name == 'target_table':
#                    # Create a target column list for target upsert/append
#                    target_column_list = ""
#                    for column in self.target_columns:
#                        for key, value in column.items():
#                            target_column_list += key + ','
#                    target_column_list.strip(',')
#                    delete_clause = None
#                    from_table = str(self.etl[etl_name]['from'])
#                    if 'mode' not in self.etl[etl_name]:
#                        raise Exception("Target Table mode is not set")
#                    mode = self.etl[etl_name]['mode'].lower()
#                    if mode == 'overwrite':
#                        self.etl[etl_name]['query'] = self.pq.gen_etl_tgt_overwrite(
#                            self.target_table, from_table, target_column_list)
#                    elif mode == 'update':
#                        if self.primary_keys is None:
#                            raise Exception("UPDATE mode requires PRIMARY_KEY to be set")
#                        self.etl[etl_name]['query'] = self.pq.gen_etl_tgt_update(
#                            self.target_table, from_table, target_column_list, self.primary_keys)
#                    elif mode == 'append':
#                        if 'delete_clause' in self.etl[etl_name]:
#                            delete_clause = self.etl[etl_name]['delete']
#                        self.etl[etl_name]['query'] = self.pq.gen_etl_tgt_append(
#                            self.target_table, from_table, target_column_list, delete_clause)
#                    else:
#                        raise Exception("Unknown MODE for {e}".format(e=etl_name))
                    # Add drop tmp tables statements at the end
                    self.etl[etl_name]['query'] += self.pq.gen_drop_tmp_tables(
                        self.tmp_tables)

                # Replace query with variables
#                self.etl[etl_name]['query'] = self.replace_variables(self.etl[etl_name]['query'])

                # Create Airflow tasks
                list_of_tasks.append(PythonOperator(
                    task_id=etl_name,
                    python_callable=self.__run_presto,
                    op_kwargs={'query': self.etl[etl_name]['query']},
                    dag=dag,
                    params=self.get_custom_params(), # For replacing tmp/staging tables
                ))

        # For DQ
        list_of_tasks.append(PythonOperator(
            task_id='dq',
            python_callable=self.__run_detector,
            op_kwargs={},
            dag=dag,
        ))

        return list_of_tasks

    def run_dag(self):
        ''' Public method to be used to run the DAG '''
        dag = self.create_dag()
        # Generate the queries from AirflowDAG
        self.gen_etl_query(self.pq)
        # Generate the Airflow tasks for Presto operator
        list_of_tasks = self.__gen_tasks(dag)
        # Set dependency (run in serial according to YAML configuration)
        for i, t in enumerate(list_of_tasks):
            if i < len(list_of_tasks)-1:
                list_of_tasks[i] >> list_of_tasks[i+1]
        return dag
