from airflow.providers.presto.hooks.presto import PrestoHook

from lib.detector import Detector
from lib.prestodqquery import PrestoDQQuery

class PrestoDetector(Detector):
    def __init__(self, dq_data):
        # External class file contains all ETL queries
        self.pq = PrestoDQQuery()
        # Airflow operator
        self.ph = PrestoHook()

        super().__init__(
            dq_file=None,
            dq_data=dq_data,
            dq_engine=self.ph,
            dq_queries_generator=self.pq,
        )

    def run(self):
        self.run_dq()
