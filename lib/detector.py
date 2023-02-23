import airflow
from airflow.utils.db import create_session
from airflow.models import Variable

import yaml
import sys
import time
import os
import getpass
from datetime import datetime

from lib.myemail import myEmail

class Detector():
    def __init__(self, dq_file, dq_data, dq_engine, dq_queries_generator,
        email=None, dq_run_hour=None, is_dry_run=False, is_unit_test=False, variables=[]):
        # This unique DQ key is to identify all the tests done from each run
        self.dq_key = int(time.time())
        self.dq_run_hour = datetime.now().strftime("%Y-%m-%d %H:00:00")

        # The unix username who runs the DQ
        #self.unix_username = os.getlogin()
        self.unix_username = getpass.getuser

        # Extract db host and username from URI
        self.db_host = dq_engine.get_uri().split('@')[1].split(':')[0]
        self.db_username = dq_engine.get_uri().split('@')[0].split('//')[1].split(':')[0]

        # DQ Framework variables
        self.target_table = None
        self.target_schema_name = None
        self.target_table_name = None
        self.target_filter = "1=1"
        self.source_table = None
        self.source_schema_name = None
        self.source_table_name = None
        self.source_filter = "1=1"
        self.dq_data = dq_data
        self.test_summary = []

        # Get all variables from Airflow server
        with create_session() as session:
            self.airflow_variables = {var.key: var.val for var in session.query(Variable)}

        if is_dry_run and is_unit_test:
            raise Exception("Cannot set both DRY_RUN and UNIT_TEST")

        # Print SQLs only if enabled
        self.is_dry_run = is_dry_run

        # Do not insert to meta table if enabled
        #self.is_unit_test = self.airflow_variables['unit_test']
        self.is_unit_test = is_unit_test

        #self.env = self.airflow_variables['env']
        self.env = 'NON-PROD'

        # For sending email
        self.notification_email = email # Overwrite the email from command argument
        if self.env == 'PROD':
            self.notification_email_subject_prefix = "[DQ WARNING]"
        else:
            self.notification_email_subject_prefix = "*NON-PROD* [DQ WARNING]"
        self.notification_email_subject = None
        self.notification_email_body = ""
        self.notification_email_footer = ""
        self.email = myEmail()
        self.email_from = "simonyung@upwork.com"

        # This is for enabling/disable email notification
        self.enable_email = True

        # This is for sending email if validation is failed
        self.to_send_email = False

        # This is for error out the framework if validation is failed and
        # stop_on_failure is set to True
        self.to_error_out = False

        # Meta table for the DQ test results
        #self.dq_table = "{user}.dq_data_result".format(user=self.airflow_variables['db_user'])
        self.dq_table = "dq_data_result"
        #self.dq_table_prod = "{user}.dq_data_result".format(user=self.airflow_variables['db_user'])
        self.dq_table_prod = "dq_data_result"
        self.dq_table_columns = {
            'dq_run_hour': {
                'type': 'timestamp',
                'nullable': False,
                'description': 'DQ run hour',
            },
            'schema_name': {
                'type': 'string',
                'nullable': False,
                'description': 'Schema name',
            },
            'table_name': {
                'type': 'string',
                'nullable': False,
                'description': 'Table name',
            },
            'table_filter': {
                'type': 'string(1000)',
                'nullable': True,
                'description': 'Filter for the table',
            },
            'dq_name': {
                'type': 'string',
                'nullable': False,
                'description': 'Name of the DQ',
            },
            'dq_column': {
                'type': 'string',
                'nullable': True,
                'description': 'Column for the DQ check',
            },
            'dq_description': {
                'type': 'string',
                'nullable': True,
                'description': 'Description of the DQ',
            },
            'dq_tgt_value': {
                'type': 'string(1000)',
                'nullable': False,
                'description': 'DQ result'
            },
            'dq_src_value': {
                'type': 'string(1000)',
                'nullable': True,
                'description': 'Base line for comparison',
            },
            'dq_threshold': {
                'type': 'string',
                'nullable': True,
                'description': 'Threshold for the DQ',
            },
            'is_pass': {
                'type': 'boolean',
                'nullable': False,
                'description': 'Pass or fail for the DQ',
            },
            'stop_on_failure': {
                'type': 'boolean',
                'nullable': False,
                'description': 'To error out DQ when failed',
            },
            'is_dq_custom': {
                'type': 'boolean',
                'nullable': False,
                'description': 'Is it a custom DQ or not',
            },
            'dq_key': {
                'type': 'bigint',
                'nullable': False,
                'description': 'Surrogate key for DQ check',
            },
            'dq_start_tstamp': {
                'type': 'timestamp',
                'nullable': False,
                'description': 'Start time for DQ',
            },
            'dq_end_tstamp': {
                'type': 'timestamp',
                'nullable': False,
                'description': 'End time for DQ',
            },
            'db_username': {
                'type': 'string',
                'nullable': False,
                'description': 'Database username for running DQ',
            },
            'unix_username': {
                'type': 'string',
                'nullable': False,
                'description': 'Unix username for running DQ',
            },
            'env': {
                'type': 'string',
                'nullable': False,
                'description': 'Environment run on for DQ',
            },
            'is_trial': {
                'type': 'boolean',
                'nullable': False,
                'description': 'Is it included in DQ summary',
            }
        }

        self.insert_sql = "INSERT INTO {table}".format(table=self.dq_table)

        self.dq_engine = dq_engine
        self.dq_queries_generator = dq_queries_generator

        # If file is specified, read from file
        if dq_file is not None:
            self.dq_data = self.__read_config(dq_file)
        else:
            self.dq_data = dq_data

        self.__setup_config()

    def __run_setup(self):
        ''' Execute create DQ table statement '''
        create = self.dq_engine.gen_create_table(
            self.dq_table, self.dq_table_columns
        )
        self.__run_sql(create, description="Create DQ table structure")

    def run_setup(self):
        ''' Public function to run setup when user requested '''
        self.__run_setup()
        sys.exit(0)

    def run_setup_stddev(self, is_exit=True):
        ''' Public function to insert initial data for standard deviation '''
        if 'columns' in self.dq_data['dq']['std_dev']:
            columns = self.dq_data['dq']['std_dev']['columns']
        else:
            raise Exception("ERROR: Missing column(s) for std_dev check")

        for column in columns:
            insert = self.dq_queries_generator.get_stddev_setup(
                column=column,
                vars=self.__get_class_variables()
                )
            self.__run_sql(insert, description="Insert initial data for std_dev")
        if is_exit:
            sys.exit(0)

    def __replace_variables(self, str):
        ''' To replace any string with the variables list '''
        # Replace str for from Airflow variables list
        for v in self.airflow_variables:
            var_name = v
            var_value = self.airflow_variables[v]
            str = str.replace(":{vn}".format(vn=var_name), var_value)

        return str

    def __get_class_variables(self):
        ''' This is for passing class variables to other modules '''
        d = {}
        d['target_table'] = self.target_table
        d['target_schema_name'] = self.target_schema_name
        d['target_table_name'] = self.target_table_name
        d['target_filter'] = self.target_filter
        d['source_table'] = self.source_table
        d['source_schema_name'] = self.source_schema_name
        d['source_table_name'] = self.source_table_name
        d['source_filter'] = self.source_filter
        d['insert_sql'] = self.insert_sql
        d['dq_run_hour'] = self.dq_run_hour
        d['dq_key'] = self.dq_key
        d['dq_table'] = self.dq_table
        d['dq_table_prod'] = self.dq_table_prod
        d['db_username'] = self.db_username
        d['unix_username'] = self.unix_username
        d['env'] = self.env
        return d

    def __read_config(self, config_file):
        ''' For reading and converting YAML file '''
        with open(config_file) as f:
            return yaml.load(f, Loader=yaml.FullLoader)

    def __setup_config(self):
        ''' Parse the DQ data to global variables '''
        if 'target_table' not in self.dq_data:
            raise Exception("Missing target_table")
        else:
            self.target_table = self.dq_data['target_table']['name']
            if 'filter' in self.dq_data['target_table']:
                self.target_filter = dq_data['target_table']['filter']
            self.target_table = self.__replace_variables(self.target_table)
            self.target_schema_name = self.target_table.split('.')[0]
            self.target_table_name = self.target_table.split('.')[1]
            # Add double quote for schema name with space or upper character
            if ' ' in self.target_table or \
              self.target_schema_name[0:1].isupper() or \
              self.target_table_name[0:1].isupper():
                self.target_table = '"' + self.target_table.replace('.', '"."') + '"'

        if 'source_table' in self.dq_data:
            self.source_table = self.dq_data['source_table']['name']
            if 'filter' in self.dq_data['source_table']:
                self.source_filter = self.dq_data['source_table']['filter']
            self.source_table = self.__replace_variables(self.source_table)
            self.source_schema_name = self.source_table.split('.')[0]
            self.source_table_name = self.source_table.split('.')[1]
            # Add double quote for schema name with space or upper character
            if ' ' in self.source_table or \
              self.source_schema_name[0:1].isupper() or \
              self.source_table_name[0:1].isupper():
                self.source_table = '"' + self.source_table.replace('.', '"."')  + '"'

        if 'notification_email' in self.dq_data:
            # If email is not set from command argument, use the one specified in yaml file
            if self.notification_email is None:
                self.notification_email = self.dq_data['notification_email']
            self.notification_email_subject = "{s}: {t}".format(
                    s=self.notification_email_subject_prefix,
                    t=self.target_table,
                    )
            self.email.config_email(
                email_from=self.email_from,
                email_to=self.notification_email,
                subject=self.notification_email_subject
                )
            self.notification_email_body = "<html><body>"
            self.notification_email_body += "<b>DQ Run Key</b>: {id}<br>".format(id=self.dq_key)
            self.notification_email_body += "<b>From</b>: {host}<br>".format(host=self.db_host)
            self.notification_email_body += "<b>SQL</b>: <p style=\"font-family: courier;\">"
            self.notification_email_body += "SELECT * FROM {table} WHERE dq_key = {id} AND is_pass = false</p>".format(
                table=self.dq_table,
                id=self.dq_key,
                )
            self.notification_email_body += """<b>Result</b>:
            <table>
            <head>
            <meta charset="utf-8" />
            <style type="text/css">
              table {
                border-spacing:0;
                border-collapse:collapse;
              }
              th {
                font-family:Arial, sans-serif;
                font-size:14px;
                font-weight:normal;
                font-weight:normal;padding:10px 5px;
                border-style:solid;
                border-width:1px;
                border-color:black;
              }
              td {
                font-family:Arial, sans-serif;
                font-size:14px;
                padding:10px 5px;
                border-style:solid;
                border-width:1px;
                border-color:black;
              }
              .tg-m9r4{background-color:#ffffc7;text-align:left;vertical-align:top}
              .tg-0lax{text-align:left;vertical-align:top}
             </style>
             </head>
                <tr>
                    <th class="tg-m9r4"><b>table_name</b></th>
                    <th class="tg-m9r4"><b>dq_name</b></th>
                    <th class="tg-m9r4"><b>dq_column</b></th>
                    <th class="tg-m9r4"><b>tgt_value</b></th>
                    <th class="tg-m9r4"><b>src_value</b></th>
                </tr>
            """
            self.notification_email_footer = "</table>"
            self.notification_email_footer += "</body></html>"

        if 'enable_email' in self.dq_data:
            self.enable_email = self.dq_data['enable_email']

    def run_dq(self):
        for dq_name in self.dq_data['dq']:
            if 'enabled' not in self.dq_data['dq'][dq_name]:
                raise Exception("Missing enabled option")
            if self.dq_data['dq'][dq_name]['enabled'] is True:
                if 'description' in self.dq_data['dq'][dq_name]:
                    description = self.dq_data['dq'][dq_name]['description']
                else:
                    description = ""

                # Default min threshold
                threshold_min = None
                if 'threshold' in self.dq_data['dq'][dq_name]:
                    threshold = str(self.dq_data['dq'][dq_name]['threshold'])
                    # For threshold, we can have a range specified if it's comma separated
                    if ',' in threshold:
                        threshold_min = threshold.split(',')[0]
                        threshold = threshold.split(',')[1]
                    # Valid range rules for threshold and min threshold
                    if dq_name.startswith('std_dev'):
                        if float(threshold) > 10.0 or float(threshold) < -10.0:
                            raise Exception("Unexpected threshold number (between -10.0 and 10.0)")
                        if threshold_min is not None:
                            if float(threshold_min) > 10.0 or float(threshold_min) < -10.0:
                                raise Exception("Unexpected min threshold number (between -10.0 and 10.0)")                                
                    else:                            
                        if float(threshold) > 1.0 or float(threshold) < -1.0:
                            raise Exception("Unexpected threshold number (between -1.0 and 1.0)")
                        if threshold_min is not None:
                            if float(threshold_min) > 1.0 or float(threshold_min) < -1.0:
                                raise Exception("Unexpected min threshold number (between -1.0 and 1.0)")
                else:
                    # Default threshold
                    if dq_name.startswith('std_dev'):
                        threshold = '1'
                    else:
                        threshold = '0'

                if 'columns' in self.dq_data['dq'][dq_name]:
                    columns = self.dq_data['dq'][dq_name]['columns']
                else:
                    columns = []

                if 'stop_on_failure' in self.dq_data['dq'][dq_name]:
                    stop_on_failure = self.dq_data['dq'][dq_name]['stop_on_failure']
                else:
                    stop_on_failure = False

                if 'group_by' in self.dq_data['dq'][dq_name]:
                    group_by = self.dq_data['dq'][dq_name]['group_by']
                else:
                    group_by = None

                if 'num_days' in self.dq_data['dq'][dq_name]:
                    num_days = self.dq_data['dq'][dq_name]['num_days']
                    if int(num_days) > 30:
                        raise Exception("Num of days cannot exceed more than 30")
                else:
                    num_days = None

                if 'is_trial' in self.dq_data['dq'][dq_name]:
                    is_trial = self.dq_data['dq'][dq_name]['is_trial']
                else:
                    is_trial = False

                if 'compare_type' in self.dq_data['dq'][dq_name]:
                    compare_type = self.dq_data['dq'][dq_name]['compare_type']
                else:
                    compare_type = None

                self.__run_generic_sql(
                    dq_name=dq_name,
                    threshold=threshold,
                    threshold_min=threshold_min,
                    stop_on_failure=stop_on_failure,
                    columns=columns,
                    is_trial=is_trial,
                    description=description,
                    group_by=group_by,
                    num_days=num_days,
                    compare_type=compare_type,
                    )
            else:
                print("Skipped '{dq_name}'".format(dq_name=dq_name))
                print("\n")

        if 'dq_custom' in self.dq_data:
            for dq_name in self.dq_data['dq_custom']:
                if 'enabled' not in self.dq_data['dq_custom'][dq_name]:
                    raise Exception("Missing enabled option")
                if self.dq_data['dq_custom'][dq_name]['enabled'] is True:
                    if 'description' in self.dq_data['dq_custom'][dq_name]:
                        description = self.dq_data['dq_custom'][dq_name]['description']
                    else:
                        description = ""

                    if 'stop_on_failure' in self.dq_data['dq_custom'][dq_name]:
                        stop_on_failure = self.dq_data['dq_custom'][dq_name]['stop_on_failure']
                    else:
                        stop_on_failure = False

                    if 'is_trial' in self.dq_data['dq_custom'][dq_name]:
                        is_trial = self.dq_data['dq_custom'][dq_name]['is_trial']
                    else:
                        is_trial = False

                    if 'sql_file' in self.dq_data['dq_custom'][dq_name]:
                        sql_file = self.dq_data['dq_custom'][dq_name]['sql_file']
                        # Enable environment variables in file path
                        sql_file = os.path.expandvars(sql_file)
                        custom_sql = open(sql_file, 'r').read()
                        self.__run_custom_sql(
                            dq_name=dq_name,
                            custom_sql=custom_sql,
                            stop_on_failure=stop_on_failure,
                            is_trial=is_trial,
                            description=description,
                            )
                    elif 'sql' in self.dq_data['dq_custom'][dq_name]:
                        custom_sql = self.dq_data['dq_custom'][dq_name]['sql']
                        self.__run_custom_sql(
                            dq_name=dq_name,
                            custom_sql=custom_sql,
                            stop_on_failure=stop_on_failure,
                            is_trial=is_trial,
                            description=description,
                            )
                    else:
                        raise Exception("No custom SQL or SQL file specified")
                else:
                    print("Skipped '{dq_name}'".format(dq_name=dq_name))
                    print("\n")

        self.__print_summary()

        # No email sent out for unit test and dry run
        if self.to_send_email and not self.is_unit_test and not self.is_dry_run:
            if self.notification_email is None:
                raise Exception("Notification Email is NOT set!")
            # This is for forcing the framework to fail with proper email notification
            if self.to_error_out:
                self.email.set_email_subject(self.notification_email_subject.replace('WARNING', 'FAILURE'))
                self.notification_email_footer += "<p><h2>DQ job is killed!!!</h2></p>"
            if self.enable_email is True:
                self.email.send_email(self.notification_email_body + self.notification_email_footer)
            else:
                print("===> EMAIL NOTIFICATION IS DISABLED <===")

        # Exit framework with error if validation test did not pass
        if self.to_error_out and not self.is_dry_run:
            print("{s}".format(s='*'*30))
            print("DQ job is killed!!!")
            print("{s}".format(s='*'*30))
            raise Exception("DQ Failed!")

    def __run_sql(self, sql, description=None):
        ''' To execute SQL statement in UDW '''
        if description is not None:
            print("### [{t}]: {desc} ###".format(
                t=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                desc=description,
            ))
            print(sql)
            print("\n")
        if not self.is_dry_run:
            try:
                result = self.dq_engine.run(sql)
                if result is None:
                    return None
                else:
                    return result
            except:
                #return None
                raise Exception("Found error in run_sql!")
        else:
            return None

    def __run_generic_sql(self, dq_name, threshold='0', threshold_min=None, stop_on_failure=False, columns=[], is_trial=False,
        description="", group_by=None, num_days=None, compare_type=None):
        ''' Contains/compiles all the generic SQLs to be executed '''
        sqls = []
        dq_columns = []
        generic_checks = self.dq_queries_generator

        if dq_name.startswith('trending'):
            # Any variations of trending test cases consider trending dq_name
            dq_name = 'trending'
            for column in columns:
                column = self.__replace_variables(column)
                dq_columns.append(column)
            sqls.extend(generic_checks.trending(dq_name, threshold, stop_on_failure, dq_columns, is_trial, description,
                self.__get_class_variables(), compare_type, threshold_min))               
        elif dq_name == 'compare_to_source':
            dq_columns = ['count(*)']
            sqls.extend(generic_checks.compare_to_source(dq_name, threshold, stop_on_failure, columns, is_trial, description,
                self.__get_class_variables()))
        elif dq_name == 'empty_null':
            dq_columns.append('#'.join(columns))
            sqls.extend(generic_checks.empty_null(dq_name, threshold, stop_on_failure, columns, is_trial, description,
                self.__get_class_variables()))
        elif dq_name == 'unique':
            dq_columns.append('#'.join(columns))
            sqls.extend(generic_checks.unique(dq_name, threshold, stop_on_failure, columns, is_trial, description,
                self.__get_class_variables()))
        elif dq_name == 'up_to_date':
            for column in columns:
                dq_columns.append(column)
            sqls.extend(generic_checks.up_to_date(dq_name, threshold, stop_on_failure, columns, is_trial, description,
                self.__get_class_variables()))
        elif dq_name == 'day_to_day':
            for column in columns:
                dq_columns.append(column)
            sqls.extend(generic_checks.day_to_day(dq_name, threshold, stop_on_failure, columns, is_trial, description,
                self.__get_class_variables(), group_by, num_days, threshold_min))
        elif dq_name.startswith('std_dev'):
            # Any variations of standard deviation test cases consider std_dev dq_name
            dq_name = 'std_dev'
            # For testing the INSERT statement in unit_test mode
            if self.is_unit_test:
                self.run_setup_stddev(is_exit=False)
            for column in columns:
                column = self.__replace_variables(column)
                dq_columns.append(column)
            sqls.extend(generic_checks.std_dev(dq_name, threshold, stop_on_failure, dq_columns, is_trial, description,
                self.__get_class_variables()))
        else:
            raise Exception("Unknown Generic DQ name")

        self.__execute_dq(sqls, dq_name, dq_columns, stop_on_failure)

    def __run_custom_sql(self, dq_name, custom_sql, stop_on_failure=False, is_trial=False, description=""):
        ''' For gathering any custom sql or sql file to be executed '''
        sqls = []
        dq_columns = ['']

        # Columns to return from custom sql: tgt_value, src_value, result
        sqls.append("""
            {insert}
            SELECT
                '{dq_run_hour}' AS dq_run_hour
                ,'{schema_name}' AS schema_name
                ,'{table_name}' AS table_name
                ,NULL AS table_filter
                ,'{dq_name}' AS dq_name
                ,'' AS dq_column
                ,'{desc}' AS dq_description
                ,CAST(x.tgt_value AS VARCHAR) AS dq_tgt_value
                ,CAST(x.src_value AS VARCHAR) AS dq_src_value
                ,NULL AS dq_threshold
                ,CASE WHEN x.result = 0 THEN true ELSE false END AS is_pass
                ,{stop} AS stop_on_failure
                ,True AS is_dq_custom
                ,{dq_key} AS dq_key
                ,CURRENT_TIMESTAMP AS dq_start_tstamp
                ,clock_timestamp() AS dq_end_tstamp
                ,'{db_username}' AS db_username
                ,'{unix_username}' AS unix_username
                ,'{env}' AS env
                ,{trial} AS is_trial
            FROM
                (
                {custom_sql}
                ) x
            ;
        """.format(
            insert=self.insert_sql,
            dq_run_hour=self.dq_run_hour,
            schema_name=self.target_schema_name,
            table_name=self.target_table_name,
            desc=description,
            dq_name=dq_name,
            custom_sql=custom_sql,
            stop=stop_on_failure,
            dq_key=self.dq_key,
            db_username=self.db_username,
            unix_username=self.unix_username,
            env=self.env,
            trial=is_trial,
            )
        )

        self.__execute_dq(sqls, dq_name, dq_columns, stop_on_failure)

    def __execute_dq(self, sqls, dq_name, dq_columns, stop_on_failure):
        ''' For executing the SQL in DB '''
        for i, sql in enumerate(sqls):
            # Execute DQ SQL in UDW
            sql = self.__replace_variables(sql)
            self.__run_sql(sql, "Running '{dq}'".format(dq=dq_name))

            # Get result from DQ table to see if it passes or not
            sql_test_result = 'true'
            for column in dq_columns[i].split('#'):
                if dq_name == 'day_to_day':
                    col = "AND dq_column ilike '{dq_column}%'".format(dq_column=column.replace("'", "''"))
                    test_sql = """
                        SELECT
                            CASE WHEN SUM(num_fails) = 0 THEN true ELSE false END AS is_pass,
                            '<<< check dq table >>>' AS dq_tgt_value,
                            NULL AS dq_src_value,
                            MAX(dq_threshold)
                        FROM
                            (SELECT is_pass, dq_tgt_value, dq_src_value, dq_threshold,
                            CASE WHEN not is_pass THEN 1 ELSE 0 END AS num_fails
                            FROM {dq_table}
                            WHERE dq_key = {dq_key} AND dq_name = '{dq_name}' {dq_column}) x
                        """.format(
                        dq_table=self.dq_table,
                        dq_key=self.dq_key,
                        dq_name=dq_name,
                        dq_column=col,
                        )
                else:
                    col = "AND dq_column = '{dq_column}'".format(dq_column=column.replace("'", "''"))
                    test_sql = """
                        SELECT is_pass, dq_tgt_value, dq_src_value, dq_threshold
                        FROM {dq_table}
                        WHERE dq_key = {dq_key} AND dq_name = '{dq_name}' {dq_column}
                        ORDER BY dq_end_tstamp DESC LIMIT 1
                        """.format(
                        dq_table=self.dq_table,
                        dq_key=self.dq_key,
                        dq_name=dq_name,
                        dq_column=col,
                        )

                # Check test result
                sql_test_result = self.__run_sql(test_sql, "Check result for '{dq}'".format(dq=dq_name))
                if sql_test_result is not None:
                    try:
                        sql_test_result = sql_test_result[0]
                    except:
                        print(sql_test_result)
                    if sql_test_result[0]:
                        result = 'PASS'
                    else:
                        result = 'FAIL'
                    tgt_value = sql_test_result[1]
                    if sql_test_result[2] is None:
                        src_value = ''
                    else:
                        src_value = sql_test_result[2]
                    if sql_test_result[3] is None:
                        threshold = ''
                    else:
                        threshold = sql_test_result[3]
                    print('Target value = {v}'.format(v=tgt_value))
                    print('Source value = {v}'.format(v=src_value))
                    print('TEST RESULT  = {v}'.format(v=result))
                    print('\n')

                    # Save result to summary list
                    if len(column) > 0:    
                        nm = "{dq_name} for {dq_column}".format(dq_name=dq_name, dq_column=column)
                    else:
                        nm = "{dq_name}".format(dq_name=dq_name, dq_column=column)
                    summary_str = '{:50s} : {:80s} : {:50s} : {:5s}'.format(
                        self.target_table,
                        nm,
                        "[tgt=" + tgt_value + "; src=" + src_value + "; threshold=" + threshold + "]",
                        result,
                        )
                    self.test_summary.append(summary_str)

                # Stop if DQ is failed (if enabled)
                if not self.is_dry_run:
                    if result == 'FAIL':
                        # Send email for any failure
                        self.to_send_email = True
                        self.notification_email_body += """
                        <tr>
                            <th class="tg-0lax">{tbl}</th>
                            <th class="tg-0lax">{dq}</th>
                            <th class="tg-0lax">{c}</th>
                            <th class="tg-0lax">{t}</th>
                            <th class="tg-0lax">{s}</th>
                        </tr>
                        """.format(
                            tbl=self.target_table,
                            dq=dq_name,
                            c=column,
                            t=tgt_value,
                            s=src_value,
                            )
                        if stop_on_failure:
                            self.to_error_out = True

    def __print_summary(self):
        print("{s} Data Validation Summary {s}".format(s='*'*90))
        for t in self.test_summary:
            print(t)
        print("{s}".format(s='*'*205))
