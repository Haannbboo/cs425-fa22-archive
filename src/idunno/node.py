import threading
from typing import List
from transformers import AutoFeatureExtractor, AutoModelForImageClassification

class IdunnoNode:
    def __init__(self) -> None:
        self.model_map = {}
    
    def pretrain(self, model_name):
        extractor = AutoFeatureExtractor.from_pretrained("microsoft/" + model_name)
        model = AutoModelForImageClassification.from_pretrained("microsoft/" + model_name)
        self.model_map[model_name] = (extractor, model)

    def commander(self):
        pass

    def run(self):
        threads: List[threading.Thread] = []

        threads.append(threading.Thread(target=self.commander))

        for thread in threads:
            thread.start()
