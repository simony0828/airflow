from airflow.providers.presto.hooks.presto import PrestoHook

import argparse
from lib.detector import Detector
from lib.prestodqquery import PrestoDQQuery

def parse_args():
    parser = argparse.ArgumentParser(description='Data quality framework by reading YAML configuration file')

    parser.add_argument('-f', '--file', action="store", required=True, dest='config_file',
        help="YAML file for data quality checks")
    parser.add_argument('-r', '--run_hour', action="store", required=False, dest='run_hour',
        help="Store the current running hour or any particular batch run time")
    parser.add_argument('-d', '--dry_run', action="store_true", required=False, dest='dry_run', default=False,
        help="Print all SQLs only")
    parser.add_argument('-e', '--email', action="store", required=False, dest='email',
        help="To overwrite the email specified in yaml (for testing purpose)")
    parser.add_argument('-s', '--setup', action="store_true", default=False, dest='setup',
        help="Run setup step only")
    parser.add_argument('-p', '--setup_update', action="store_true", default=False, dest='setup_update',
        help="Make changes to DQ result table")
    parser.add_argument('-t', '--setup_stddev', action="store_true", default=False, dest='setup_stddev',
        help="Initial setup for standard deviation with trending history")
    parser.add_argument('-u', '--unit_test', action="store_true", default=False, dest='unit_test',
        help="Enable data quality check without writing result to table")
    parser.add_argument('-v', '--variable', action="append", required=False, dest='variables', default=[],
        help="For SQL variables replacement")

    return parser.parse_args()

def __main__():
    # Parse command line arguments
    args = parse_args()

    # Default engine for Detector
    engine = PrestoHook()
    generator = PrestoDQQuery()

    # Create Detector object
    d = Detector(
        dq_run_hour=args.run_hour,
        dq_file=args.config_file,
        dq_data=None,
        dq_engine=engine,
        dq_queries_generator=generator,
        email=args.email,
        is_dry_run=args.dry_run,
        is_unit_test=args.unit_test,
        variables=args.variables,
        )

    # If setup flag is set, only run the setup part
    if args.setup:
        d.run_setup()

    # If setup update flag is set, only run the statements to update dq result table
    if args.setup_update:
        d.run_setup_update()

    # If setup for stddev is set, only run the insert for trending history data
    if args.setup_stddev:
        d.run_setup_stddev()

    d.run_dq()


__main__()
