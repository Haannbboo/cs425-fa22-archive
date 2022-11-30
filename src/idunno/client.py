import socket
from src.sdfs import SDFS, Message
import time
from typing import Any, Union, List
from src.config import * 
import pickle
import threading
import os
import numpy as np
from tqdm import tqdm

class IdunnoClient(SDFS):
    
    def __init__(self) -> None:
        super().__init__() # init sdfs
    
    def pretrain_request(self, model_name):
        train_message = self.__generate_message("REQ TRAIN", content={"model_name": model_name})
        targets = [i.host for i in self.all_processes]
        train_ack = []
        
        for target in targets:
            threading.Thread(target=self.write_to, args=(train_message, target, PRE_TRAIN_PORT, True)).start()
            
                    
                   
    def write_to(self, message, host, port, response: bool = True) -> Union[Message, int]:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:  # tcp
            # print(f"... ... Write {message.message_type} to {remote_host}")
            try:
                s.connect((host, port))
                s.sendall(pickle.dumps(message))
                s.shutdown(socket.SHUT_WR)
            except socket.error:
                return 0
            if response:
                # wait for confirm
                data = s.recv(1024)
                resp: Message = pickle.loads(data)
                return resp
            return 1

    def send_inference(self, model_name: str, data_dir: str, batch_size: int):
        # Read local dataset and upload to sdfs
        data_files = os.listdir(data_dir)
        sdfs_fname = [f"{model_name} {fname}" for fname in data_files]

        # Send to coordinator
        to_host, to_port = self.__get_coordinator_addr()
        message = self.__generate_message("NEW JOB", content={"model_name": model_name, "batch_size": batch_size, "dataset": sdfs_fname})
        confirmed: Message = self.write_to(message, to_host, to_port)
        if confirmed:  # Message does not implement __bool__
            return True
        else:
            return False

    def upload(self, model_name: str, data_dir: str):
        data_files = os.listdir(data_dir)
        data_fpath = [os.path.join(data_dir, fname) for fname in data_files]
        sdfs_fname = [model_name + fname for fname in data_files]
        i = 0
        print("... Putting inference dataset to sdfs")
        with tqdm(total=len(data_files)) as pbar:
            while i < len(data_files):
                confirmed = self.put(data_fpath[i], sdfs_fname[i])
                if confirmed:
                    i += 1
                    pbar.update(1)

    def recv_completion(self):
        """Receives job completion from coordinator."""
            
            
    def run(self):
        """
        Commands:
        =========
        - train [models name]
        - upload [model name] [input directory]
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
                self.pretrain_request(argv[1:])
            elif argv[0] == "upload" and len(argv) >= 3:
                # upload dataset to sdfs
                model_name, data_dir = argv[1], argv[2]
                self.upload(model_name, data_dir)
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
                    

            elif argv[0] == "state" and len(argv) > 1:
                #show the job's state (demo C1)
                continue
            elif argv[0] == "result" and len(argv) > 1:
                #show the result of given job (demo C4)
                continue
            elif argv[0] == "assign" or argv[0] == "C5":
                #show the current set of VMs assigned to each job (demo C5)
                message = self.__generate_message("C5")
                to_host, to_port = self.__get_coordinator_addr()
                resp: Message = self.write_to(message, to_host, to_port)  # resp from coordinator
                print(resp.content["resp"])
            elif argv[0] == "report" or argv[0] == "C2" and len(argv) >= 2:
                # get current processing time
                message = self.__generate_message("C2", content={"job_name": argv[1]})
                to_host, to_port = self.__get_coordinator_addr()
                resp: Message = self.write_to(message, to_host, to_port)
                ptime: List[float] = resp.content["resp"]  # list of processing time
                # Calculate average, percentiles, std
                ptime = np.array(ptime)
                n_completed = len(ptime)
                average, std, median = np.average(ptime), np.std(ptime), np.median(ptime)
                percentiles = np.percentile(ptime, [90, 95, 99])
                print(f"Completed: {n_completed}")
                print(f"\tProcessing time: average {average}\tstd {std}\tmedian {median}\t90% {percentiles[0]}\t95% {percentiles[1]}\t99% {percentiles[2]}")
            else:
                print(f"[ERROR] Invalid command: {command}")

    def __get_coordinator_addr(self):
        message = self.__generate_message("coordinator")
        resp = self.ask_dns(message)
        host, port = resp.content["host"], resp.content["port"]
        return (host, port)
    