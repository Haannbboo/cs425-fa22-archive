import threading
from typing import List

class IdunnoNode:

    def commander(self):
        pass

    def run(self):
        threads: List[threading.Thread] = []

        threads.append(threading.Thread(target=self.commander))

        for thread in threads:
            thread.start()
