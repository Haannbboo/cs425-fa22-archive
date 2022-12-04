import os
import pickle
import socket
import time
from threading import Thread
from typing import List, Dict, Any

from src.config import *
from src.sdfs import SDFS, Message
from .scheduling import FairTimeScheduler
from .utils import JobTable, Job, Query, QueryTable
from .node import BaseNode


class IdunnoCoordinator(BaseNode):

    def __init__(self) -> None:
        self.jobs = JobTable()
        self.scheduler = FairTimeScheduler()

        self.available_workers: List[int] = []

        self.standby_addr = None

    def run(self) -> List[Thread]:
        self.__remove_tmp_files()
        
        threads = []
        
        threads.append(Thread(target=self.client_server))
        threads.append(Thread(target=self.standby_recv))
        threads.append(Thread(target=self.job_dispatch))
        threads.append(Thread(target=self.job_collection))
        threads.append(Thread(target=self.failure_recv))

        return threads

    def submit_job(self, job: Job) -> bool:
        if not self.__admission_control(job):
            return False

        if not self.__notify_new_job(job):
            return False
        self.jobs.append(job)
        
        # Tell avaialble workers that a new job arrived,
        # start working!
        message = self.__generate_message("START")
        self.sdfs.multicast(message, set(self.available_workers), PORT_START_WORKING)

        return True

    def standby_recv(self):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            s.bind(("", PORT_STANDBY_UPDATE))
            s.listen()

            while True:           
                conn, addr = s.accept()
                with conn:
                    data = self.sdfs.recv_message(conn)
                    message: Message = pickle.loads(data)

                    if message.message_type == "QUERIES UPDATE":
                        # Some queries have completed processing
                        queries: List[Query] = message.content["queries"]  # len > 0
                        job: Job = self.jobs[queries[0].job_id]
                        for query in queries:  # move from idle to completed
                            try:
                                job.queries.idle_queries.remove(query)
                            except ValueError:
                                continue
                            else:
                                job.queries.completed_queries.append(query)
                            
                        job.queries.completed_queries.sort(key=lambda query: query.scheduled_time, reverse=True)
                        confirm = self.__generate_message("UPDATE CONFIRM")
                        conn.sendall(pickle.dumps(confirm))

                    elif message.message_type == "JOB UPDATE":
                        job: Job = message.content["job"]
                        if job not in self.jobs:
                            self.jobs.append(job)
                        confirm = self.__generate_message("UPDATE CONFIRM")
                        conn.sendall(pickle.dumps(confirm))

    def failure_recv(self):
        """Receives failure from DNS (maybe?) and handle worker failure."""
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            s.bind(("", PORT_COORDINATOR_FAILURE_LISTEN))
            s.listen()
            while True:
                conn, _ = s.accept()
                with conn:
                    data = conn.recv(4096)

                    if data:
                        message: Message = pickle.loads(data)
                        
                        failed_worker_id = message.content["id"]
                        print(f"Coordinator received {failed_worker_id} FAILURE message")
                        self.__handle_worker_failure(failed_worker_id)
                    

    def client_server(self):
        """Serves client's request."""
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            s.bind(("", PORT_IDUNNO_COORDINATOR))
            s.listen()

            while True:
                conn, addr = s.accept()
                with conn:
                    data = self.sdfs.recv_message(conn)
                    message: Message = pickle.loads(data)
                    
                    if message.message_type == "C1":
                        rates = self.__get_processing_rate()
                        resp = self.__generate_message("RESP C1", content={"resp": rates})
                        conn.sendall(pickle.dumps(resp))

                    elif message.message_type == "C2":
                        job_name = message.content["job_name"]
                        ptime = self.__get_processing_time(job_name)
                        if ptime is None:
                            resp = self.__generate_message("ERROR")
                        else:
                            resp = self.__generate_message("RESP C2", content={"resp": ptime})
                        conn.sendall(pickle.dumps(resp))
                    
                    elif message.message_type == "C5":
                        placement = self.__get_job_placement()
                        resp = self.__generate_message("RESP C5", content={"resp": placement})
                        conn.sendall(pickle.dumps(resp))

                    elif message.message_type == "NEW JOB":
                        # New inference job requested
                        # TODO: might need to resp first then processing request

                        # Generate new job
                        job = self.__welcome_client(message)
                        print(f"... New job requested: {job.name}")
                        self.submit_job(job)
                        
                        confirmation = self.__generate_message("NEW JOB CONFIRM")
                        conn.sendall(pickle.dumps(confirmation))

                    elif message.message_type == "jobs":
                        # Return JobTable
                        resp = self.__generate_message("RESP jobs", content={"resp": repr(self.jobs)})
                        conn.sendall(pickle.dumps(resp))

                    elif message.message_type == "completed":
                        # Return last 10 completed queries for each job
                        completed = {}
                        for job in self.jobs:
                            completed[job.name] = job.queries.completed_queries[:10]
                        resp = self.__generate_message("RESP completed", content={"resp": completed})
                        conn.sendall(pickle.dumps(resp))


    def job_dispatch(self):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            s.bind(("", PORT_REQUEST_JOB))
            s.listen()
            while True:
                conn, addr = s.accept()
                with conn:
                    data = self.sdfs.recv_message(conn)
                    message: Message = pickle.loads(data)
                    
                    if message.message_type == "REQ QUERIES":
                        job = self.scheduler.schedule(self.jobs)
                        if job is None:  # no active job
                            self.send_stop(conn)  # tell worker to rest
                            self.available_workers.append(message.id)
                        else:
                            queries = job.queries.get_idle_queries(job.batch_size)
                            ack = self.send_queries(queries, conn)
                            for query in queries:
                                query.expected_complete_time = time.time() + job.batch_size * job.avg_inf_time + 0.3
                            if ack:  # if worker has received works to do
                                job.queries.mark_as_scheduled(queries)
                                self.jobs.placement[message.id] = queries
                            else:  # worker doesn't receive the job
                                job.queries.mark_as_idle(queries)  # put work back to pool


    def job_collection(self):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            s.bind(("", PORT_COMPLETE_JOB))
            s.listen()
            
            while True:
                conn, addr = s.accept()
                with conn:
                    data = self.sdfs.recv_message(conn)
                    message: Message = pickle.loads(data)

                    if message.message_type == "COMPLETE QUERIES":
                        self.recv_completion(message)


    def send_queries(self, queries: List[Query], s: socket.socket) -> bool:
        message = self.__generate_message("RESP QUERIES", content={"queries": queries})
        try:
            s.sendall(pickle.dumps(message))
            s.shutdown(socket.SHUT_WR)
        except socket.error as e:
            return False

        try:
            s.settimeout(1)
            # ack = s.recv(32)
        except socket.timeout:
            return False
        return True

    def send_stop(self, s: socket.socket) -> None:
        message = self.__generate_message("STOP")
        s.sendall(pickle.dumps(message))

    def recv_completion(self, message: Message) -> bool:
        queries: List[Query] = message.content["queries"]
        if len(queries) == 0:
            return False

        print(f"Current time: {round(time.time(), 1)}, actual complete time: {round(queries[0].complete_time)}")

        job: Job = self.jobs[queries[0].job_id]
        # Write first, then update JobTable.
        # Check result file upon completion and remove duplicates.
        
        if self.__write_queries_result(queries, job) and self.__notify_queries_completed(queries):
            job.queries.mark_as_completed(queries)

        if self.__job_completed(job):
            self.__drop_result_duplicates(job)  # there should only be duplicates
            if self.__notify_client_job_completed(job):  # if client confirmed job completion
                self.__handle_job_complete(job)
        print(f"Finish processing: {round(time.time(), 1)}")
        return True

    def __admission_control(self, new_job: Job) -> bool:
        """Decides if ``new_job`` could be admitted."""
        return True

    def __write_queries_result(self, queries: List[Query], job: Job) -> bool:
        """Writes result of ``queries`` to output file on sdfs."""
        fname = self.__job_to_sdfs_fname(job)
        if self.sdfs.exists(fname):
            success = self.sdfs.get(fname, fname)
            if not success or not self.__local_file_ready(fname):
                print(f"[ERROR] Failed to write queries result to sdfsfile {fname}")
                return False
        else:
            # If result file not in SDFS, create it in local
            open(fname, "wb").close()

        result = [f"{query.id}) {self.__query_to_output_key(query)} |--| {query.result}\n" 
                    for query in queries if query.result is not None]
        if len(result) == 0:
            return False

        with open(fname, "ab") as f:
            f.write("".join(result).encode('utf-8'))
        
        return self.sdfs.put(fname, fname)

    def __job_completed(self, job: Job) -> bool:
        """Detects if ``job`` has completed or not. 
        It's ok to have duplicates in the result file at this stage."""
        return job.queries.total_queries == job.queries.completed

    def __handle_job_complete(self, job: Job):
        """Defines what to do when a job is completed and confirmed by the client."""
        job.completed = True
        job.running = False
        # Remove tmp output file from sdfs and local
        fname = self.__job_to_sdfs_fname(job)
        self.sdfs.delete(fname)
        if os.path.exists(fname):
            os.remove(fname)

    def __drop_result_duplicates(self, job: Job) -> bool:
        """Drops duplicates in the result file."""
        # No need to sdfs get, since local version should be fresh.
        fname = self.__job_to_sdfs_fname(job)
        query_ids = set()
        open(job.output_file, "wb").close()  # clear local output file
        output_file = open(job.output_file, "ab")
        drop_cnt = 0  # number of duplicates
        with open(fname, "rb") as f:
            for _l in f:
                line = _l.decode('utf-8')
                if line is not None and len(line) > 0:
                    query_id = line.split(")")[0]
                    if query_id in query_ids:  # duplicate
                        drop_cnt += 1
                        continue  # ignore

                    query_ids.add(query_id)
                    output_file.write(line.encode('utf-8'))
        output_file.close()
        self.sdfs.put(job.output_file, job.output_file)  # upload result file
        print(f"... Job {job.name} dropped {drop_cnt} duplicates")
        return True

    def __handle_worker_failure(self, worker_id: int):
        """What to do when ``worker_id`` failed."""
        if worker_id not in self.jobs.placement:
            return
        
        queries = self.jobs.placement[worker_id]
        if len(queries) > 0:
            job_id = queries[0].job_id
            job = self.jobs[job_id]
            job.queries.mark_scheduled_queries_as_idle(queries)
        del self.jobs.placement[worker_id]  # remove from placement

    def __notify_queries_completed(self, queries: List[Query]) -> bool:
        """Notify standby coordinator that some ``queries`` has completed. Confirmation needed."""
        if len(queries) == 0:
            return False
        standby_addr = self.__get_standby_coordinator_host()
        if standby_addr[0] == "":  # no standby
            return True
        message = self.__generate_message("QUERIES UPDATE", content={"queries": queries})
        confirm = self.sdfs.write_to(message, standby_addr[0], standby_addr[1])
        return bool(confirm)

    def __notify_new_job(self, job: Job) -> bool:
        """Notify standby coordinator that a new ``job`` has been submitted. Confirmation needed."""
        standby_addr = self.__get_standby_coordinator_host()
        if standby_addr[0] == "":  # no standby
            return True
        message = self.__generate_message("JOB UPDATE", content={"job": job})
        confirm = self.sdfs.write_to(message, standby_addr[0], standby_addr[1])
        return bool(confirm)

    def __notify_client_job_completed(self, job: Job) -> bool:
        """Notify client that a ``job`` has completed. Needs client's confirmation."""
        message = self.__generate_message("JOB COMPLETE", content={"job": job})
        confirm = self.sdfs.write_to(message, job.client[0], job.client[1])
        return bool(confirm)

    def __get_standby_coordinator_host(self) -> tuple:
        """Gets standby coordinator host."""
        # If already has standby coordinator addr, just return
        if self.standby_addr is not None:
            return self.standby_addr
        # otherwise, ask dns for this
        else:
            message = self.__generate_message("standby")
            resp = self.sdfs.ask_dns(message)
            host = resp.content["host"]
            return host, PORT_STANDBY_UPDATE

    ### Client side commands
    def __get_processing_time(self, job_name: int) -> List[float]:  # command C2
        job: Job = self.jobs.get_job_by_name(job_name)
        if job is None:
            return None
        completed_queries = job.queries.completed_queries[:]
        processing_time = [query.processing_time for query in completed_queries]
        return processing_time

    def __get_job_placement(self) -> Dict[int, str]:  # command C5
        # Output: vm -> job name
        placement = {k: v[0].job_name for k, v in self.jobs.placement.items() if len(v) > 0}
        return placement

    def __get_processing_rate(self) -> Dict[str, List]:  # command C1
        return {job.name: [job.queries.processed, job.rate] for job in self.jobs if job.running}
        
    def __welcome_client(self, message: Message) -> Job:
        """Parses client's new job request message. Returns a formatted ``Job``."""
        model_name = message.content["model_name"]
        batch_size = message.content["batch_size"]
        sdfs_fname: List[str] = message.content["dataset"]
        job = self.jobs.generate_new_job()
        job.client = message.host, PORT_IDUNNO_CLIENT
        job.batch_size = int(batch_size)
        job.model = model_name
        job.name = model_name  # for now job name is model_name
        job.output_file = model_name  # for now
        
        queries = QueryTable()
        for fname in sdfs_fname:
            queries.add_query(job.id, job.name, job.model, fname)
        job.queries = queries
        return job

    ### Utility functions
    @staticmethod
    def __local_file_ready(fname: str) -> bool:
        return os.path.exists(fname)

    @staticmethod
    def __query_to_output_key(query: Query) -> str:
        return query.input_file

    @staticmethod
    def __job_to_sdfs_fname(job: Job) -> str:
        return job.output_file + ".tmp"

    @staticmethod
    def __remove_tmp_files():
        files = os.listdir()
        for fname in files:
            if fname.endswith("tmp"):
                os.remove(fname)

    def __generate_message(self, m_type: str, content: Any = None) -> Message:
        """Generates message for all communications."""
        return Message(self.sdfs.id, self.sdfs.host, self.sdfs.port, time.time(), m_type, content)
