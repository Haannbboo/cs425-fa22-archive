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
    scheduled_time: float = 0.0  # when this query is scheduled by coordinator
    complete_time: float = 0.0  # when this query is completed

    expected_complete_time: float = 0.0  # estimated complete time

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

    @property
    def processed(self) -> int:
        """Runing count, since the start of the model."""
        return len(self.completed_queries) + len(self.scheduled_queries)

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
            query.scheduled_time = time.time()
            self.scheduled_queries.append(query)
            self.hold_queries.remove(query)

    def mark_as_completed(self, queries: List[Query]):
        for query in queries:
            query.complete_time = time.time()
            self.completed_queries.append(query)
            self.scheduled_queries.remove(query)
        self.completed_queries.sort(key=lambda query: query.complete_time, reverse=True)

    def mark_scheduled_queries_as_idle(self, queries: List[Query]):
        for query in queries:
            if query in self.scheduled_queries:  # this might be useful, but who knows
                self.idle_queries.append(query)
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

    start_time: float = time.time()

    # States
    running: bool = True
    completed: bool = False

    @property
    def avg_inf_time(self) -> float:
        if self.completed == 0:
            return 0.3
        sum_ptime = 0
        for query in self.queries.completed_queries:
            sum_ptime += query.processing_time
        return sum_ptime / self.completed

    @property
    def rate(self) -> float:
        """Number of queries / second"""
        if self.queries.completed == 0:
            return 0
        # Count queries that started within last 10 seconds
        completed = 0
        now = time.time()
                
        return self.queries.completed / (time.time() - self.start_time)


class JobTable:
    # Stores current jobs and their allocations

    def __init__(self) -> None:
        self.jobs: List[Job] = []
        self.placement: Dict[int, List[Query]] = {}

        # Statistics
        self.rate_diff: List[float] = []
        self.rate_diff_timestamps: List[float] = []

        self.max_job_id = 0

    def __contains__(self, job_id: int) -> bool:
        for job in self.jobs:
            if job_id == job.id:
                return True
        return False

    def __len__(self) -> int:
        return len(self.jobs)

    def __iter__(self):
        yield from self.jobs

    def __getitem__(self, job_id: int) -> Job:
        for job in self.jobs:
            if job.id == job_id:
                return job
        raise ValueError(f"No job with id {job_id}")

    def __repr__(self) -> str:
        ret = []
        for job in self.jobs:
            ret.append(f"<Job {job.id}>")
        return "\n".join(ret)

    def generate_new_job(self) -> Job:
        new_job = Job(self.max_job_id)
        self.max_job_id += 1
        return new_job

    def get_job_by_name(self, job_name: str) -> Job:
        ret: Job = None
        for job in self.jobs:
            if job.name == job_name and (ret is None or job.id > ret.id):
                ret = job
        return ret

    def append(self, job: Job):
        self.jobs.append(job)
