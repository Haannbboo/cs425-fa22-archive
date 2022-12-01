from typing import List

from .utils import JobTable, Job


class FairTimeScheduler:

    def schedule(self, jobs: JobTable) -> Job:
        """Selects the running job with lowest ``rate`` property."""
        selected_job = None
        min_rate = 100000  # arbitrary large number
        for job in jobs:
            if job.rate < min_rate and job.running and len(job.queries) > 0:
                min_rate = job.rate
                selected_job = job

        return selected_job