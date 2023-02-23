import airflow
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.utils.db import create_session
from airflow.models import Variable

import os
import yaml
from yamlinclude import YamlIncludeConstructor
from datetime import timedelta
from jinja2 import Template
from lib.watcher import Watcher

class AirflowDAG():
    def __init__(self, dag_id, yaml_file, description=""):
        # Default setting
        self.default_email = 'simony0828@aim.com'

        # Airflow variables
        self.dag_id = dag_id
        self.dag_description = description
        self.default_args = {
            'owner': 'admin',
            'start_date': airflow.utils.dates.days_ago(1),
            'depends_on_past': False,
            'email': [self.default_email],
            'email_on_failure': True,
            'email_on_retry': True,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),
        }
        self.schedule_interval = '@daily'
        self.start_date = days_ago(1)

        # YAML configuration variables
        self.target_table = None
        self.target_columns = []
        self.primary_keys = None
        self.partition_keys = None
        self.source_tables = []
        self.wait_tables = []
        self.staging_tables = {}
        self.tmp_tables = {}
        self.etl = {}
        self.config_data = {}

        # DQ setting
        self.file_path = os.path.dirname(os.path.abspath(yaml_file))
        self.dq_data = None

        # Watcher setting
        self.watcher_data = None

        self.__setup_config(yaml_file)

    def __read_config(self, config_file):
        ''' For reading and converting YAML file '''
        YamlIncludeConstructor.add_to_loader_class(loader_class=yaml.FullLoader,
            base_dir=self.file_path)
        with open(config_file) as f:
            return yaml.load(f, Loader=yaml.FullLoader)

    def __setup_config(self, config_file):
        ''' Parse the YAML file to global variables '''
        self.config_data = self.__read_config(config_file)
        #print(self.config_data)

        if 'target_table' not in self.config_data:
            raise Exception("Missing target_table")
        else:
            self.target_table = self.config_data['target_table']['name']
            self.target_columns = self.config_data['target_table']['columns']

        if 'primary_key' in self.config_data['target_table']:
            self.primary_keys = ','.join(self.config_data['target_table']['primary_key'])

        if 'partition_key' in self.config_data['target_table']:
            self.partition_keys = ','.join(self.config_data['target_table']['partition_key'])

        sleep_time = 30 # DEFAULT: 30 mins
        max_retry = 3 # DEFAULT: num retries
        if 'watcher' in self.config_data:
            if 'sleep_time' in self.config_data['watcher']:
                sleep_time = self.config_data['watcher']['sleep_time']
            if 'max_retry' in self.config_data['watcher']:
                max_retry = self.config_data['watcher']['max_retry']

        if 'source_tables' in self.config_data:
            source_tables = self.config_data['source_tables']['name']
            for table in source_tables:
                # Check if we want to skip watcher as we would get a dict type
                if isinstance(table, dict):
                    for i, t in enumerate(table):
                        self.source_tables.append(t)
                        if 'skip_watcher' in t:
                            if k['skip_watcher'] is False:
                                self.wait_tables.append(t)
                else:
                    self.source_tables.append(table)
                    self.wait_tables.append(table)

        if len(self.wait_tables) > 0:
            self.watcher_data = {
                'sleep_time': sleep_time,
                'max_retry': max_retry,
                'tables': self.wait_tables
            }

        if 'staging_tables' in self.config_data:
            for i, t in enumerate(self.config_data['staging_tables']):
                table = t['name']
                table_name = table.split('.')[-1]
                columns = t['columns']
                self.staging_tables[table_name] = {}
                self.staging_tables[table_name]['columns'] = columns
                self.staging_tables[table_name]['table'] = table

        if 'etl' not in self.config_data:
            raise Exception("Missing etl")
        else:
            self.etl = self.config_data['etl']
            for e in self.etl:
                if 'enabled' in self.etl[e]:
                    # Remove step from dictionary if not enabled
                    if self.etl[e]['enabled'] == False:
                        del self.etl[e]
                # Read sql_file if specified
                if 'sql_file' in self.etl[e]:
                    sql_file = self.file_path + "/" + self.etl[e]['sql_file']
                    self.etl[e]['sql'] = open(sql_file, 'r').read()

        if 'dq' in self.config_data:
            # Read dq yaml_file if specified
            dq_filename = self.config_data['dq']['yaml_file']
            dq_file = self.file_path + "/" + dq_filename
            self.dq_data = self.__read_config(dq_file)
        else:
            # Auto DQ default setting
            self.dq_data = {
                'target_table': {'name': self.target_table},
                'notification_email': self.default_email,
                'dq': {
                    'trending': {
                        'enabled': True,
                        'stop_on_failure': False,
                        'columns': ['count(*)'],
                        'threshold': 0.3
                    },
                    'up_to_date': {
                        'enabled': False,
                        'stop_on_failure': False,
                        'columns': ['dw_modified_ts']
                    },
                }
            }
            # Set unique check based on primary keys
            if self.primary_keys is not None:
                self.dq_data['dq'].update({
                    'unique': {
                        'enabled': True,
                        'stop_on_failure': True,
                        'columns': self.primary_keys.split(',')
                    }
                })
            # Set empty_null check based on columns nullable option
            null_columns = []
            for column in self.target_columns:
                for key, value in column.items():
                    if value.get('nullable', True):
                        null_columns.append(key)
            if len(null_columns) > 0:
                self.dq_data['dq'].update({
                    'empty_null': {
                        'enabled': True,
                        'stop_on_failure': True,
                        'columns': null_columns
                    }
                })

    def replace_variables(self, str):
        ''' To replace any string with the variables list '''
#        # Use jinga2 Template to replace table reference names
#        tmp_dict = {}
#        for staging_table in self.staging_tables:
#            tmp_dict[staging_table] = self.staging_tables[staging_table]['table']
#        for tmp_table in self.tmp_tables:
#            tmp_dict[tmp_table] = self.tmp_tables[tmp_table]['table']
#        tm = Template(str)
#        str = tm.render(tmp_dict)
#
        # Get all variables from Airflow server
#        with create_session() as session:
#            variables = {var.key: var.val for var in session.query(Variable)}
#        # Replace str for from Airflow variables list
#        for v in variables:
#            var_name = v
#            var_value = variables[v]
#            str = str.replace(":{vn}".format(vn=var_name), var_value)
#
#        return str
        pass

    def get_custom_params(self):
        ''' To replace tmp and staging tables with Jinja as custom variables '''
        tmp_dict = {}
        for staging_table in self.staging_tables:
            tmp_dict[staging_table] = self.staging_tables[staging_table]['table']
        for tmp_table in self.tmp_tables:
            tmp_dict[tmp_table] = self.tmp_tables[tmp_table]['table']
        return tmp_dict

    def run_watcher(self):
        ''' Run watcher '''
        watcher = Watcher(watcher_file=None, watcher_data=self.watcher_data)
        watcher.run()

    def gen_etl_query(self, query_engine):
        ''' Generate the ETL queries based on the engine '''
        for etl_name in self.etl:
            # This is to process tmp table step
            if etl_name.startswith("tmp_"):
                etl_sql = self.etl[etl_name]['sql']
                self.tmp_tables[etl_name] = {}
                self.tmp_tables[etl_name]['table'] = etl_name
                self.etl[etl_name]['query'] = query_engine.gen_etl_tmp(
                    self.tmp_tables[etl_name]['table'], etl_sql)
            # This is to process staging/intermediate table step
            elif etl_name.startswith("stg_"):
                etl_sql = self.etl[etl_name]['sql']
                etl_table = self.staging_tables[etl_name]['table']
                delete_clause = None
                if 'delete' in self.etl[etl_name]:
                    delete_clause = self.etl[etl_name]['delete']
                self.etl[etl_name]['query'] = query_engine.gen_etl_stg(
                    etl_table, etl_sql, delete_clause)
            # This is to process final target table process
            elif etl_name == 'target_table':
                target_column_list = ""
                for column in self.target_columns:
                    for key, value in column.items():
                        target_column_list += key + ','
                target_column_list = target_column_list.rstrip(',')
                delete_clause = None
                if 'delete' in self.etl[etl_name]:
                    delete_clause = self.etl[etl_name]['delete']
                if 'from' not in self.etl[etl_name]:
                    raise Exception("Target Table requires FROM")
                from_table = str(self.etl[etl_name]['from'])
                if 'mode' not in self.etl[etl_name]:
                    raise Exception("Target Table mode is not set")
                else:
                    mode = self.etl[etl_name]['mode'].lower()
                    if mode == 'overwrite':
                        print("*** OVERWRITE ***")
                        self.etl[etl_name]['query'] = query_engine.gen_etl_tgt_overwrite(
                            self.target_table, from_table, target_column_list)
                    elif mode == 'update':
                        print("*** UPDATE ***")
                        if self.primary_keys is None:
                            raise Exception("UPDATE mode requires PRIMARY_KEY to be set")
                        self.etl[etl_name]['query'] = query_engine.gen_etl_tgt_update(
                            self.target_table, from_table, target_column_list, self.primary_keys)
                    elif mode == 'append':
                        print("*** APPEND ***")
                        if 'delete_clause' in self.etl[etl_name]:
                            delete_clause = self.etl[etl_name]['delete']
                        self.etl[etl_name]['query'] = query_engine.gen_etl_tgt_append(
                            self.target_table, from_table, target_column_list, delete_clause)
                    else:
                        raise Exception("Unknown MODE for {e}".format(e=etl_name))
            # This is to process bash commands/script
            elif etl_name.startswith("sh_"):
                bash_commmand = None
                if 'script_file' in self.etl[etl_name]:
                    script_file = self.file_path + "/" + self.etl[etl_name]['script_file']
                    print("Reading file: {f}".format(f=script_file))
                    bash_commmand = open(script_file, 'r').read()
                else:
                    bash_commmand = self.etl[etl_name]['script']
                self.etl[etl_name]['bash_commmand'] = bash_commmand
            else:
                raise Exception("Unknown ETL name")

            # Replace query with variables
#            self.etl[etl_name]['query'] = self.replace_variables(self.etl[etl_name]['query'])

    def run_detector(self, detector, **kwargs):
        ''' Run detector '''
        detector.run()

    def create_dag(self):
        ''' Create inital DAG to run in Airflow '''
        return DAG(
            self.dag_id,
            description=self.dag_description,
            default_args=self.default_args,
            schedule_interval=self.schedule_interval,
            start_date=self.start_date,
        )
