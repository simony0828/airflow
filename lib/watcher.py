import airflow
from airflow.utils.db import create_session
from airflow.models import Variable

import yaml
import time
from datetime import datetime

class Watcher:
    def __init__(self, watcher_file, watcher_data,
        is_dry_run=False, is_unit_test=False, variables=[]):
        self.wait_tables = []      
        self.total_count = 0
        self.env = 'NON-PROD'

        # Print SQLs only if enabled
        self.is_dry_run = is_dry_run
        # Reset max_retry and sleep_time for unit_test mode
        self.is_unit_test = is_unit_test

        if self.is_unit_test:
            self.sleep_time = 5/60 # 5 secs
            self.max_retry = 2
        #else:            
        #    self.sleep_time = 30 # DEFAULT: 30 mins
        #    self.max_retry = 3 # DEFAULT: num retries
        self.retry = 1 # keep track of retry number

        # Variables replacement (ie: rundeck repo)
        self.variables = variables

        # If file is specified, read from file
        if watcher_file is not None:
            self.watcher_data = self.__read_config(watcher_file)
        else:
            self.watcher_data = watcher_data

        # Load .yaml config
        self.__setup_config()   

        print("MAX RETRY  = {r}".format(r=self.max_retry))
        print("SLEEP TIME = {r}".format(r=self.sleep_time))
        print("\n")

        if self.is_dry_run:
            print("*** DRY RUN ***")
        if self.is_unit_test:
            print("*** UNIT TEST ***")

    def __read_config(self, config_file):
        ''' For reading and converting YAML file '''
        with open(config_file) as f:
            return yaml.load(f, Loader=yaml.FullLoader)

    def __replace_variables(self, str):
        ''' To replace any string with the variables list '''
        # Replace str for from Airflow variables list
        for v in self.airflow_variables:
            var_name = v
            var_value = self.airflow_variables[v]
            str = str.replace(":{vn}".format(vn=var_name), var_value)

        return str
        
    def __setup_config(self):
        ''' Parse the YAML file to global variables '''
        # Ignore these settings if it is unit_test
        if not self.is_unit_test:
            if 'sleep_time' in self.watcher_data:
                self.sleep_time = self.watcher_data['sleep_time']

            # This is the valid sleep wait time (in minutes)
            if self.sleep_time >= 15 and self.sleep_time <= 60:
                pass
            else:
                raise Exception("Sleep Time can only between 15 to 60 mins")

            if 'max_retry' in self.watcher_data:
                self.max_retry = self.watcher_data['max_retry']

            # This is the valid number of retries
            if self.max_retry >= 0 and self.max_retry <= 12:
                pass
            else:
                raise Exception("Max Retry can only between 0 and 12 times")

        if 'tables' in self.watcher_data:
            self.wait_tables = self.watcher_data['tables']

        self.total_count = len(self.wait_tables)

    def run(self):
        ''' Main function to see if upstream tasks/tables are done '''
        tables_status = {}
        wait_count = 0

        print("[{t}] Try #{n} (out of {m}):\n".format(
            t=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            n=self.retry,
            m=self.max_retry,
        ))

        # Check upstream tasks from sherlock.task_log table
        if self.env == 'PROD':
            schema = 'sherlock'
        else:
            schema = 'sherlock_stage'
        task_filter = "','".join(self.wait_tables)
        sql = """
            SELECT task, MAX(log_time)::VARCHAR AS end_time
            FROM {s}.task_log
            WHERE action = 'end'
            AND task in ('{f}')
            GROUP BY 1
        """.format(f=task_filter, s=schema)
        wait_count = self.__check_table(sql, self.wait_tables, "upstream tables", wait_count)

        # Determine if we have the tasks/tables ready to proceed or not
        # If not, continue to wait
        if wait_count > 0:
            if self.retry < self.max_retry:
                self.retry += 1
                print("\n")
                print("[{t}] Continue to wait...".format(
                    t=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    ))
                print("{n1} Ready; {n2} Waiting".format(
                    n1=self.total_count-wait_count,
                    n2=wait_count,
                    ))
                print("\n")
                time.sleep(self.sleep_time*60)
                self.run_watcher()
            else:
                print("\n")
                raise Exception("Max retry ({m}) is reached. Exit now.".format(m=self.max_retry))
        else:
            return

    def run_watcher(self):
        self.run()

    def __check_table(self, check_sql, check_list, check_type, wait_count):
        ''' Internal function to do the checking against UDW table '''
        check_status = {}
        today = datetime.now().strftime("%Y-%m-%d")

        if len(check_list) > 0:
            # Preset status for a list of tasks/tables
            for task in check_list:
                if task not in check_status:
                    check_status[task] = None

            if self.is_dry_run:
                print("*** From check_table() ***")
                print(check_sql)
            else:
                # Print check query for the first try only
                if self.retry == 1:
                    print(check_sql)
                result = self.udw.query(check_sql)
                if len(result) > 0:
                    for row in result:
                        check_status[row[0]] = row[1]

                    for task in check_status:
                        # If task/table not found, raise an exception
                        if check_status[task] is None:
                            raise Exception("Unable to get time for '{t}'".format(t=task))
                        status_date = (check_status[task])[:10]
                        if status_date >= today:
                            status = 'READY [{t}]'.format(t=check_status[task])
                        else:
                            status = 'WAITING [current={t1} not >= today={t2}]'.format(
                                t1=status_date, t2=today)
                            wait_count += 1
                        print(task + " => " + status)
                else:
                    raise Exception("Unable to find all the {ct}!".format(ct=check_type))

        return wait_count
