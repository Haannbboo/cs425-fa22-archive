from dataclasses import dataclass, field
from typing import List, Dict


@dataclass
class Query:
    worker: int = -1  # worker id
    model: int = -1  # model id
    sdfsfile: str = None
    completion_time: float = 0.0


@dataclass
class Job:
    id: int = 0  # job id
    name: str = None  # job name
    queries: List[Query] = field(default_factory=list)
    model: int = -1  # model id
    scheduled_workers: List[int] = field(default_factory=list)
    queries_placement: Dict[int, List[Query]] = field(default_factory=dict)


class JobTable:
    # Stores current jobs and their allocations

    def __init__(self) -> None:
        self.jobs: List[Job] = []

    def __len__(self) -> int:
        return len(self.jobs)

    def __iter__(self):
        yield from self.jobs

    def append(self, job: Job):
        self.jobs.append(job)
