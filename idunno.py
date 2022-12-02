import sys
import time

from src.sdfs import SDFS, DNSserver
from src.idunno import IdunnoClient

argc = len(sys.argv)
argv = sys.argv

if argv[1] == "sdfs":
    sdfs = SDFS()
    sdfs.run()
elif argv[1] == "dns":
    d = DNSserver()
    d.run()
elif argv[1] == "client":
    client = IdunnoClient(coordinator_on=True)
    client.run()
    time.sleep(1)
    client.join()
    # client.pretrain_request("resnet-50")
    # client.pretrain_request("beit-base-patch16-224-pt22k-ft22k")
    # client.upload("train2")
    print("\n")
    # client.send_inference("resnet-50", "train2", 4)
    # client.send_inference("beit-base-patch16-224-pt22k-ft22k", "train2", 4)
elif argv[1] == "coordinator":
    coordinator = IdunnoClient(coordinator_on=True)
    coordinator.run()
