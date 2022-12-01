import threading
from typing import List
import socket
from transformers import AutoFeatureExtractor, AutoModelForImageClassification
import pickle
from PIL import Image
from typing import Any
import time

from src.config import *
from src.sdfs import SDFS, Message
from .utils import JobTable, Job, Query


RUNNING = 1
IDLE = 0

class IdunnoNode():
    def __init__(self, sdfs) -> None:
        self.sdfs = sdfs
        self.model_map = {}
        self.worker_state = IDLE
        self.coordinator_host = ""
        self.coordinator_port = ""
    
    def pretrain(self, model_name):
        print("... pretrianing ", model_name)
        extractor = AutoFeatureExtractor.from_pretrained("microsoft/" + model_name)
        model = AutoModelForImageClassification.from_pretrained("microsoft/" + model_name)
        self.model_map[model_name] = (extractor, model)
        print("finish one pretrain")
    
    #only receive START message to turn IDLE to RUNNING
    def turnON(self):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            s.bind(("", PORT_START_WORKING))
            s.listen()
            
            while True:
                conn, _ = s.accept()
                with conn:
                    data = conn.recv(4096)
                    message: Message = pickle.loads(data)
                    if message.message_type == "START":
                        self.coordinator_host = message.host
                        self.worker_state = RUNNING
    
    #only receive train request
    def receive_train_request(self):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            s.bind(("", PRE_TRAIN_PORT))
            s.listen()
            
            while True:
                conn, _ = s.accept()
                with conn:
                    data = conn.recv(4096)
                    message: Message = pickle.loads(data)
                    
                    if message.message_type == "REQ TRAIN":
                        model_name = message.content["model_name"]
                        self.pretrain(model_name)
                        
                        train_ACK = self.__generate_message("TRAIN CONFIRM")
                        
                        conn.sendall(pickle.dumps(train_ACK)) 
                    
    
    def inference_result(self, query: Query):
        model_name = query.model
        extractor, model = self.model_map[model_name]
        image = Image.open(query.input_file)
        inputs = extractor(image, return_tensors="pt")
        outputs = model(**inputs)
        logits = outputs.logits
        predicted_class_idx = logits.argmax(-1).item()
        res = model.config.id2label[predicted_class_idx]
        query.result = res
        
    
    def request_job(self):
        queries = []
        req_job_message = self.__generate_message("REQ QUERIES")
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            #if coordinator fail, what to do
            while True:
                if self.worker_state != RUNNING:
                    continue
                try: 
                    addr = (self.coordinator_host, PORT_REQUEST_JOB)
                    s.connect(addr)
                    s.sendall(pickle.dumps(req_job_message))
                    s.shutdown(socket.SHUT_WR)
                except socket.error:
                    #send message to DNS to get new coordinator host id?
                    return False
                
                try:
                    s.settimeout(1)
                    data = s.recv(4096)
                    message: Message = pickle.loads(data)
                    if message.message_type == "RESP QUERIES":
                        queries = message.content["queries"]
                        for query in queries:
                            fname = query.input_file
                            self.get(fname, fname)
                            self.inference_result(query)
                            s.sendall(b'\1') #ack
                    elif message.message_type == "STOP":
                        self.worker_state == IDLE
                    s.shutdown(socket.SHUT_WR)
                except socket.error:
                    return False
                self.job_complete(queries)
                        
                        
                
    def job_complete(self, queries):
        if self.worker_state != RUNNING:
            return False
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            addr = (self.coordinator_host, PORT_COMPLETE_JOB)
            complete_message = self.__generate_message("COMPLETE QUERIES", content={"queries": queries})
            try: 
                s.connect(addr)
                s.sendall(pickle.dumps(complete_message))
                s.shutdown(socket.SHUT_WR)
            except socket.error:
                #send message to DNS to get new coordinator host id?
                return False
        return True
    
    def __generate_message(self, m_type: str, content: Any = None) -> Message:
        """Generates message for all communications."""
        return Message(self.sdfs.id, self.sdfs.host, self.sdfs.port, time.time(), m_type, content)
                

    def run(self):
        threads: List[threading.Thread] = []

        # threads.append(threading.Thread(target=self.commander))
        threads.append(threading.Thread(target=self.receive_train_request))
        threads.append(threading.Thread(target=self.turnON))
        threads.append(threading.Thread(target=self.request_job))
        
        return threads