import sys

from src.sdfs import SDFS, DNSserver
from src.idunno import IdunnoClient, IdunnoNode, IdunnoCoordinator

argc = len(sys.argv)
argv = sys.argv

if argv[1] == "sdfs":
    sdfs = SDFS()
    sdfs.run()
elif argv[1] == "dns":
    d = DNSserver()
    d.run()
elif argv[1] == "client":
    client = IdunnoClient()
    client.run()
elif argv[1] == "worker":
    node = IdunnoNode()
    node.run()
elif argv[1] == "coordinator":
    standby = argv[2] == "standby"
    coordinator = IdunnoCoordinator(standby)
    coordinator.run()
