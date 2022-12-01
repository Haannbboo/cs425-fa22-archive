import time
from dataclasses import dataclass
from typing import List, Dict


@dataclass
class Query:
    id: int = 0
    job_id: int = 0
    job_name: str = None
    model: str = -1  # model_name
    input_file: str = None
    worker: int = -1  # worker id
    result: str = None  # inference result
    processing_time: float = 0.0

    def __eq__(self, __o: object) -> bool:
        return self.id == __o.id


class QueryTable:
    
    def __init__(self) -> None:
        self.idle_queries: List[Query] = []
        self.hold_queries: List[Query] = []
        self.scheduled_queries: List[Query] = []
        self.completed_queries: List[Query] = []

        self.max_query_id = 0

    def __len__(self) -> int:
        return len(self.idle_queries)

    @property
    def total_queries(self) -> int:
        return len(self.idle_queries) + len(self.hold_queries) + len(self.scheduled_queries) + len(self.completed_queries)

    @property
    def completed(self) -> int:
        return len(self.completed_queries)

    def add_query(self, job_id: int, job_name: str, model_name: str, sdfsfname: str):
        new_query = Query(self.max_query_id, job_id, job_name, model_name, sdfsfname)
        self.max_query_id += 1
        self.idle_queries.append(new_query)

    def get_idle_queries(self, n_queries: int) -> List[Query]:
        n_queries = min(n_queries, len(self.idle_queries))
        scheduled = self.idle_queries[:n_queries]
        for query in scheduled:
            self.hold_queries.append(query)
            self.idle_queries.remove(query)
        if len(scheduled) == 0:
            raise ValueError("Something werid with scheduling idle queries... n_queries should not be zero.")
        return scheduled 

    def mark_as_idle(self, queries: List[Query]):
        for query in queries:
            self.idle_queries.append(query)
            self.hold_queries.remove(query)

    def mark_as_scheduled(self, queries: List[Query]):
        for query in queries:
            self.scheduled_queries.append(query)
            self.hold_queries.remove(query)

    def mark_as_completed(self, queries: List[Query]):
        for query in queries:
            self.completed_queries.append(query)
            self.scheduled_queries.remove(query)


@dataclass
class Job:
    id: int = 0  # job id
    name: str = None  # job name
    queries: QueryTable = QueryTable()
    model: str = None  # model name, e.g. resnet-50
    output_file: str = None
    client: tuple = None  # issuer's host, port
    batch_size: int = 1

    start_time: float = -1

    # States
    running: bool = True
    completed: bool = False

    @property
    def rate(self) -> float:
        """Number of queries / second"""
        return (time.time() - self.start_time) / self.queries.completed


class JobTable:
    # Stores current jobs and their allocations

    def __init__(self) -> None:
        self.jobs: List[Job] = []
        self.placement: Dict[int, List[Query]] = {}

    def __len__(self) -> int:
        return len(self.jobs)

    def __iter__(self):
        yield from self.jobs

    def __getitem__(self, job_id: int) -> Job:
        for job in self.jobs:
            if job.id == job_id:
                return job
        raise ValueError(f"No job with id {job_id}")

    @property
    def highest_job_id(self):
        if len(self.jobs) == 0:
            return 0
        return max(self.jobs, key=lambda x: x.id).id

    def generate_new_job(self) -> Job:
        return Job(self.highest_job_id)

    def get_job_by_name(self, job_name: str) -> Job:
        for job in self.jobs:
            if job.name == job_name:
                return job
        return None

    def append(self, job: Job):
        self.jobs.append(job)
