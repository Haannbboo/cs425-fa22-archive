from typing import Dict, List

from src.sdfs import SDFS
from .scheduling import NaiveScheduler
from .utils import JobTable, Job, Query


class IdunnoCoordinator:

    def __init__(self, standby: bool = False) -> None:
        self.jobs = JobTable()
        self.scheduler = NaiveScheduler()
        self.sdfs = SDFS()

        self.standby = standby

    @property
    def n_workers(self) -> int:
        # TODO: handle when coordinator fails
        return len(self.sdfs.all_processes) - 2

    def submit_job(self, job: Job) -> bool:
        if not self.__admission_control(job):
            return False
        self.jobs = self.scheduler.schedule(job, self.jobs, self.n_workers)

        placement = self.__placement_policy(self, self.jobs)

    def run(self):
        pass

    def __admission_control(self, new_job: Job) -> bool:
        """Decides if ``new_job`` could be admitted."""
        return True

    def __placement_policy(self, jobs: JobTable) -> Dict[int, List[Query]]:
        pass
