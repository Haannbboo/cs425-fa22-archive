import sys

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
    client = IdunnoClient(coordinator_on=False)
    client.run()
elif argv[1] == "coordinator":
    coordinator = IdunnoClient(coordinator_on=True)
    coordinator.run()
