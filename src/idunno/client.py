import socket
import time
from typing import Any, Union, List , Dict
import pickle
import threading
import os
import numpy as np
from tqdm import tqdm

from src.sdfs import Message
from src.config import *
from .utils import Job
from .coordinator import IdunnoCoordinator
from .node import IdunnoNode, BaseNode


class IdunnoClient(BaseNode):
    
    def __init__(self, coordinator_on: bool = True) -> None:
        self.coordinator = IdunnoCoordinator()
        self.worker = IdunnoNode()
        
        self.coordinator_on = coordinator_on
        
    def run(self):
        threads = self.sdfs.run()  # run sdfs
        if self.coordinator_on:
            threads.extend(self.coordinator.run())
        threads.extend(self.worker.run())
        threads.append(threading.Thread(target=self.commander))
        threads.append(threading.Thread(target=self.recv_completion))

        for thread in threads:
            thread.start()

    def pretrain_request(self, model_name):
        train_message = self.__generate_message("REQ TRAIN", content={"model_name": model_name})
        targets = [i.host for i in self.sdfs.all_processes]
        
        for target in targets:
            # threading.Thread(target=self.write_to, args=(train_message, target, PRE_TRAIN_PORT, True)).run()
            res = self.sdfs.write_to(train_message, target, PRE_TRAIN_PORT)
            if res == 0:
                print("pre train ERROR occur")
                return 0
        return 1
    
    def write_with_resp(self, message, host, port, response: bool = True) -> Union[Message, int]:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:  # tcp
            try:
                s.connect((host, port))
                s.sendall(pickle.dumps(message))
                s.shutdown(socket.SHUT_WR)
            except socket.error:
                return 0
            if response:
                # wait for confirm
                data = bytearray()
                while True:
                    packet = s.recv(4 * 1024)
                    if not packet:
                        break
                    data.extend(packet)
                resp: Message = pickle.loads(data)
                return resp
            return 1

    def send_inference(self, model_name: str, data_dir: str, batch_size: int):
        # Read local dataset and upload to sdfs
        data_files = os.listdir(data_dir)

        # Send to coordinator
        to_host, to_port = self.__get_coordinator_addr()
        message = self.__generate_message("NEW JOB", content={"model_name": model_name, "batch_size": batch_size, "dataset": data_files})
        confirmed: Message = self.sdfs.write_to(message, to_host, to_port)
        if confirmed:  # Message does not implement __bool__
            return True
        else:
            return False

    def upload(self, data_dir: str, n: int):
        if not os.path.exists(data_dir):
            print(f"[ERROR] No such directory: {data_dir}")
            return
        data_files = os.listdir(data_dir)
        data_fpath = [os.path.join(data_dir, fname) for fname in data_files]
        i = 0
        print("... Putting inference dataset to sdfs")
        with tqdm(total=min(len(data_files), n)) as pbar:
            while i < min(len(data_files), n):
                confirmed = self.sdfs.put(data_fpath[i], data_files[i])
                if confirmed:
                    i += 1
                    pbar.update(1)
        print("... Dataset upload completed")

    def recv_completion(self):
        """Receives job completion from coordinator."""
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            s.bind(("", PORT_IDUNNO_CLIENT))
            s.listen()

            while True:
                time.sleep(1)
                conn, addr = s.accept()
                with conn:
                    data = self.sdfs.recv_message(conn)
                    message: Message = pickle.loads(data)

                    if message.message_type == "JOB COMPLETE":
                        # Coordinator tells client that a job has completed
                        job: Job = message.content["job"]
                        if self.__confirm_job_completion(job):
                            confirmation = self.__generate_message("JOB COMPLETE CONFIRM")
                            conn.sendall(pickle.dumps(confirmation))
                        if self.sdfs.get(job.output_file, job.output_file):
                            print(f"\nJob {job.name} completed! Results written to {job.output_file}")
                        else:
                            print(f"\nJob {job.name} completed, but failed to retreive to local file.")
            
    def commander(self):
        """
        Commands:
        =========
        - train [models name]
        - upload [input directory] [n]
        - inference [model name] [input directory] [batch size]
        
        """
        print()
        print("IDunno version 0.0.1")

        while True:
            command = input(">>> ")
            argv = command.split()
            if len(argv) == 0:
                continue
            elif argv[0] == "train" and len(argv) > 1:
                #send message to all worker directly? 
                self.pretrain_request(argv[1])
            elif argv[0] == "upload" and len(argv) >= 2:
                # upload dataset to sdfs
                data_dir = argv[1]
                n = 1e9  # arbitrary large number
                if len(argv) >= 3:
                    n = int(argv[2])
                self.upload(data_dir, n)
            elif argv[0] == "inference" and len(argv) >= 3:
                # inference resnet-50 train/ 4
                #inference specific task by specific model in given batch size
                
                model_name, data_dir = argv[1], argv[2]
                if len(argv) == 3:  # batch size not specified
                    batch_size = 1
                else:
                    batch_size = argv[3]
                
                if not os.path.exists(data_dir):
                    print("... Invalid directory given")
                    continue

                self.send_inference(model_name, data_dir, batch_size)

            elif argv[0] == "result" and len(argv) > 1:
                #show the result of given job (demo C4)
                continue
            
            ### Demo commands
            elif argv[0] == "C1":
                # Show: i) 10s window query rate, ii) number of queries processed
                message = self.__generate_message("C1")
                to_host, to_port = self.__get_coordinator_addr()
                resp: Message = self.write_with_resp(message, to_host, to_port)
                job_rates: Dict[str, List] = resp.content["resp"]

                if len(job_rates) == 0:
                    continue
                
                # pretty print
                max_len = len(max(job_rates, key=lambda k: len(k)))
                print(f"{'Job'.center(max_len)}\tProcessed\tRate")
                print(f"{'-'*max_len}\t---------\t----")
                for job in job_rates:
                    print(f"{job.center(max_len)}\t{str(job_rates[job][0]).center(9)}\t{round(job_rates[job][1], 4)}")

            elif argv[0] == "assign" or argv[0] == "C5":
                #show the current set of VMs assigned to each job (demo C5)
                message = self.__generate_message("C5")
                to_host, to_port = self.__get_coordinator_addr()
                resp: Message = self.write_with_resp(message, to_host, to_port)  # resp from coordinator
                print(resp.content["resp"])

            elif argv[0] == "report" or argv[0] == "C2" and len(argv) >= 2:
                # get current processing time
                message = self.__generate_message("C2", content={"job_name": argv[1]})
                to_host, to_port = self.__get_coordinator_addr()
                resp: Message = self.write_with_resp(message, to_host, to_port)

                if resp.message_type == "ERROR":  # no model for that name found
                    print(f"[ERROR] No model with name: {argv[1]}")
                    continue

                ptime: List[float] = resp.content["resp"]  # list of processing time
                # Calculate average, percentiles, std
                ptime = np.array(ptime)
                average, std, median = np.average(ptime), np.std(ptime), np.median(ptime)
                percentiles = np.percentile(ptime, [90, 95, 99])
                print(f"Processing time: average {round(average, 4)}\tstd {round(std, 4)}\tmedian {round(median, 4)}\t90% {round(percentiles[0], 4)}\t95% {round(percentiles[1], 4)}\t99% {round(percentiles[2], 4)}")
            
            ### Useful commands
            elif argv[0] == "jobs":
                message = self.__generate_message("jobs")
                to_host, to_port = self.__get_coordinator_addr()
                resp: Message = self.write_with_resp(message, to_host, to_port)
                print(resp.content["resp"])

            elif argv[0] == "completed":
                # print the last 10 completed queries
                message = self.__generate_message("completed")
                to_host, to_port = self.__get_coordinator_addr()
                resp: Message = self.write_with_resp(message, to_host, to_port)
                content: Dict[str, List] = resp.content["resp"]
                
                if len(resp.content) == 0:
                    continue
                    
                # pretty print
                for job_name in content:
                    print(f"{job_name}:")
                    for query in content[job_name]:
                        print(query)
                    print()

            elif argv[0] == "plot" and len(argv) >= 2 and argv[1] == "diff":
                t = -1
                if len(argv) >= 3:
                    t = argv[2]
                
                message = self.__generate_message("rate_diff")
                to_host, to_port = self.__get_coordinator_addr()
                resp: Message = self.write_with_resp(message, to_host, to_port)
                rate_diff: List[float] = resp.content["rate_diff"]
                timestamps: List[float] = resp.content["timestamps"]
                # if len(timestamps) > 0:
                #     t0 = min(timestamps)
                #     for i in range(len(timestamps)):
                #         timestamps[i] -= t0

                import matplotlib.pyplot as plt
                data = list(zip(timestamps, rate_diff))
                plt.scatter(timestamps, rate_diff)
                plt.xlabel("Time(s)")
                plt.ylabel("Query rate difference (%)")
                plt.savefig("plot_diff.png")

            ### ML commands
            elif argv[0] == "join":
                self.join()
            elif argv[0] == "ml":
                print(self.sdfs.fd.ml)
            else:
                print(f"[ERROR] Invalid command: {command}")

    def join(self):
        self.sdfs.join()

    def __get_coordinator_addr(self):
        message = self.__generate_message("coordinator")
        resp = self.sdfs.ask_dns(message)
        host = resp.content["host"]
        return (host, PORT_IDUNNO_COORDINATOR)

    def __confirm_job_completion(self, job: Job) -> bool:
        """Checks if a ``job`` is satisfiable, aka ready for return."""
        return True

    def __generate_message(self, m_type: str, content: Any = None) -> Message:
        """Generates message for all communications."""
        return Message(self.sdfs.id, self.sdfs.host, self.sdfs.port, time.time(), m_type, content)
    