import pickle
import socket
from typing import Dict, List, Any

from src.sdfs import SDFS, Message
from .scheduling import FairTimeScheduler
from .utils import JobTable, Job, Query


class IdunnoCoordinator(SDFS):

    def __init__(self, standby: bool = False) -> None:
        super().__init__()  # init sdfs
        self.jobs = JobTable()
        self.scheduler = FairTimeScheduler()

        self.standby = standby

    def submit_job(self, job: Job) -> bool:
        if not self.__admission_control(job):
            return False

        self.jobs.append(job)

    def send_queries(self, queries: List[Query], to_id: int, s: socket.socket) -> bool:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:  # tcp
            addr = (self.all_processes[to_id].host, self.all_processes[to_id].host)
            message = self.__generate_message("RESP QUERIES", content={"queries": queries})
            try:
                s.connect(addr)
                s.sendall(pickle.dumps(message))
                s.shutdown(socket.SHUT_WR)
            except socket.error:
                return False
            
            try:
                s.settimeout(1)
                ack = s.recv(32)
            except socket.timeout:
                return False
        return True

    def recv_completion(self):
        pass

    def job_dispatch(self):
        pass

    def job_collection(self):
        pass

    def run(self):
        pass

    def __admission_control(self, new_job: Job) -> bool:
        """Decides if ``new_job`` could be admitted."""
        return True

