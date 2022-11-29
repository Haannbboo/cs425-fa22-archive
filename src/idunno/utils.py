import time
from dataclasses import dataclass
from typing import List, Dict


@dataclass
class Query:
    id: int = 0
    worker: int = -1  # worker id
    model: int = -1  # model id
    input_file: str = None
    output_file: str = None
    completion_time: float = 0.0


class QueryTable:
    
    def __init__(self) -> None:
        self.completed = 0
        self.idle_queries: List[Query] = []
        self.hold_queries: List[Query] = []
        self.scheduled_queries: List[Query] = []
        self.completed_queries: List[Query] = []

    def __len__(self) -> int:
        return len(self.idle_queries)

    def get_idle_queries(self, n_queries: int) -> List[Query]:
        n_queries = min(n_queries, len(self.idle_queries))
        scheduled = self.idle_queries[:n_queries]
        for query in scheduled:
            self.hold_queries.append(query)
            self.idle_queries.remove(query)
        if len(scheduled) == 0:
            raise ValueError("Something werid with scheduling idle queries... n_queries should not be zero.")
        return scheduled 

    def mark_as_scheduled(self, queries: List[Query]):
        for query in queries:
            self.scheduled_queries.append(query)
            self.hold_queries.remove(query)


@dataclass
class Job:
    id: int = 0  # job id
    name: str = None  # job name
    queries: QueryTable = QueryTable()
    model: int = -1  # model id

    start_time: float = -1

    # States
    running: bool = True

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

    def append(self, job: Job):
        self.jobs.append(job)
