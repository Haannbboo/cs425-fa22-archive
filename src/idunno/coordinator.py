import os
import pickle
import socket
from typing import List

from src.config import *
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

    def job_dispatch(self):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            s.bind(("", PORT_REQUEST_JOB))
            s.listen()

            while True:
                conn, addr = s.accept()
                with conn:
                    data = self.__recv_message(s)
                    message: Message = pickle.loads(data)

                    if message.message_type == "REQ QUERIES":
                        n_queries = message.content["n_queries"]
                        job = self.scheduler.schedule(self.jobs, n_queries)
                        queries = job.queries.get_idle_queries(n_queries)
                        ack = self.send_queries(queries, message.host, message.port)
                        if ack:
                            job.queries.mark_as_scheduled(queries)


    def job_collection(self):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            s.bind(("", PORT_COMPLETE_JOB))
            s.listen()
            
            while True:
                conn, addr = s.accept()
                with conn:
                    data = self.__recv_message(s)
                    message: Message = pickle.loads(data)

                    if message.message_type == "COMPLETE QUERIES":
                        self.recv_completion(message)


    def send_queries(self, queries: List[Query], worker_host: str, worker_port: int) -> bool:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:  # tcp
            addr = (worker_host, worker_port)
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

    def recv_completion(self, message: Message) -> bool:
        queries: List[Query] = message.content["queries"]
        if len(queries) == 0:
            False

        job: Job = self.jobs[queries[0].job_id]
        # Write first, then update JobTable.
        # Check result file upon completion and remove duplicates.
        if self.__write_queries_result(queries, job):
            job.queries.mark_as_completed(queries)
        return True

    def run(self):
        pass

    def __admission_control(self, new_job: Job) -> bool:
        """Decides if ``new_job`` could be admitted."""
        return True

    def __write_queries_result(self, queries: List[Query], job: Job) -> bool:
        fname = job.output_file
        success = self.get(fname, fname)
        if not success or not self.__local_file_ready(fname):
            print(f"[ERROR] Failed to write queries result to sdfsfile {fname}")
            return False

        result = [f"{self.__query_to_output_key(query)} |--| {query.result}\n" 
                    for query in queries if query.result is not None]
        if len(result) == 0:
            return False

        with open(fname, "ab") as f:
            f.write("".join(result).encode('utf-8'))
        
        return self.put(fname, fname)

    ### Utility functions
    @staticmethod
    def __local_file_ready(fname: str) -> bool:
        return os.path.exists(fname)

    @staticmethod
    def __query_to_output_key(query: Query) -> str:
        return query.input_file
