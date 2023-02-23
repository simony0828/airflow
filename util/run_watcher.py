import argparse
from lib.watcher import Watcher

def parse_args():
    parser = argparse.ArgumentParser(description='Watcher for a list of tasks/tables by reading YAML configuration file')

    parser.add_argument('-f', '--file', action="store", required=True, dest='config_file',
        help="YAML file for tables or tasks readiness check")
    parser.add_argument('-d', '--dry_run', action="store_true", default=False, dest='dry_run',
        help="Print all SQLs only")
    parser.add_argument('-u', '--unit_test', action="store_true", default=False, dest='unit_test',
        help="Enable watcher without waiting too long")
    parser.add_argument('-v', '--variable', action="append", required=False, dest='variables', default=[],
        help="For variables replacement")
    return parser.parse_args()

def __main__():
    # Parse command line arguments
    args = parse_args()

    # Create Watcher object
    w = Watcher(
        watcher_file=args.config_file,
        watcher_data=None,
        is_dry_run=args.dry_run,
        is_unit_test=args.unit_test,
        variables=args.variables,
        )

    w.run_watcher()

__main__()