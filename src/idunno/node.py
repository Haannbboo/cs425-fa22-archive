import threading
from typing import List
import socket
from transformers import AutoFeatureExtractor, AutoModelForImageClassification
import pickle
from PIL import Image

from src.config import *
from src.sdfs import SDFS, Message
from .utils import JobTable, Job, Query

RUNNING = 1
IDLE = 0

class IdunnoNode(SDFS):
    def __init__(self) -> None:
        super().__init__()  # init sdfs
        self.model_map = {}
        self.n_queries = 1 #default number
        self.worker_state = IDLE
        self.coordinator_host = ""
        self.coordinator_port = ""
    
    def pretrain(self, model_name):
        extractor = AutoFeatureExtractor.from_pretrained("microsoft/" + model_name)
        model = AutoModelForImageClassification.from_pretrained("microsoft/" + model_name)
        self.model_map[model_name] = (extractor, model)
    
    #only receive START message to turn IDLE to RUNNING
    def turnON(self):
        pass
    
    #only receive STOP message to turn RUNNING to IDLE
    def turnOff(self):
        pass
    
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
        
    
    def request_job(self, coordinator_host, coordinator_port):
        req_job_message = self.__generate_message("REQ QUERIES", content={"n_queries": self.n_queries})
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            addr = (coordinator_host, coordinator_port)
            #if coordinator fail, what to do
            try: 
                s.connect()
                s.sendall(pickle.dumps(req_job_message))
                s.shutdown(socket.SHUT_WR)
            except socket.error:
                #send message to DNS to get new coordinator host id?
                return False
            
            try:
                s.settimeout(1)
                data = s.recv(4096)
                message: Message = pickle.loads(data)
                queries = []
                if message.message_type == "RESP QUERIES":
                    queries = message.content["queries"]
                    for query in queries:
                        fname = query.input_file
                        self.get(fname, fname)
                        self.inference_result(query)
                s.sendall(b'\1') #ack
                s.shutdown(socket.SHUT_WR)
                self.job_complete(queries, coordinator_host, coordinator_port)
            except socket.error:
                return False
                        
                        
                
    def job_complete(self, queries, coordinator_host, coordinator_port):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            addr = (coordinator_host, PORT_COMPLETE_JOB)
            complete_message = self.__generate_message("COMPLETE QUERIES", content={"queries": queries})
            try: 
                s.connect()
                s.sendall(pickle.dumps(complete_message))
                s.shutdown(socket.SHUT_WR)
                self.request_job(coordinator_host, coordinator_port)
            except socket.error:
                #send message to DNS to get new coordinator host id?
                return False
                
                

    def commander(self):
        pass

    def run(self):
        threads: List[threading.Thread] = []

        threads.append(threading.Thread(target=self.commander))

        for thread in threads:
            thread.start()
