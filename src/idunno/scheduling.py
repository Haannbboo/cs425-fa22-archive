from .utils import Job, JobTable


class NaiveScheduler:

    def schedule(self, new_job: Job, old_jobs: JobTable, n_workers: int) -> JobTable:
        n_jobs = len(old_jobs) + 1
        n = n_workers // n_jobs
        new_job_workers = []
        for job in old_jobs:
            new_job_workers.extend(job.scheduled_workers[n:][:])
            job.scheduled_workers = job.scheduled_workers[:n]
        new_job.scheduled_workers = new_job_workers
        old_jobs.append(new_job)
        return old_jobs