import socket
from src.sdfs import SDFS, Message
import time
from typing import Any
from src.config import * 
import pickle
import threading

class IdunnoClient(SDFS):
    
    def __init__(self) -> None:
        super().__init__() # init sdfs
    
    def pretrain_request(self, model_name):
        train_message = self.__generate_message("REQ TRAIN", content={"model_name": model_name})
        targets = [i.host for i in self.all_processes]
        train_ack = []
        
        for target in targets:
            threading.Thread(target=self.write_to, args=(train_message, target, PRE_TRAIN_PORT, True)).start()
            
                    
                   
    def write_to(self, message, host, port, confirm):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:  # tcp
            # print(f"... ... Write {message.message_type} to {remote_host}")
            try:
                s.connect((host, port))
                s.sendall(pickle.dumps(message))
                s.shutdown(socket.SHUT_WR)
            except socket.error:
                return 0
            if confirm:
                # wait for confirm
                data = s.recv(1024)
                confirmation: Message = pickle.loads(data)
                confirmed = confirmation.message_type.endswith("CONFIRM")
                return confirmed
            return 1
            
            
    def run(self):
        print()
        print("IDunno version 0.0.1")

        while True:
            command = input(">>> ")
            argv = command.split()
            if len(argv) == 0:
                continue
            elif argv[0] == "train" and len(argv) > 1:
                #send message to all worker directly? 
                self.pretrain_request(argv)
            elif argv[0] == "if" and len(argv) > 2:
                #inference specific task by specific model in given batch size
                continue
            elif argv[0] == "state" and len(argv) > 1:
                #show the job's state (demo C1)
                continue
            elif argv[0] == "result" and len(argv) > 1:
                #show the result of given job (demo C4)
                continue
            elif argv[0] == "assign":
                #show the current set of VMs assigned to each job (demo C5)
                continue
            else:
                print(f"[ERROR] Invalid command: {command}")
    