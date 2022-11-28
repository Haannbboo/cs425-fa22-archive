### Ports ###
PORT_SDFS = 8765  # SDFS receiver/sender
PORT_FDPING = 8766  # Failure detector ping/ack
PORT_FDSERVER = 8767  # Failure detector receiver
PORT_FDUPDATE = 9000  # Failure detector update port
PORT_FDINTRODUCER = 8887  # Introducer port
PORT_SDFS_GETFAILURE = 9001 #where sdfs get failure message
INTRODUCER_HOST = "fa22-cs425-1301.cs.illinois.edu"

DNS_SERVER_HOST = "fa22-cs425-1301.cs.illinois.edu"
DNS_SERVER_PORT = 8769

# Test ports #
PORT_TEST_UDPSERVER = 9213

PORT_TCP_CLIENTMASTER = 8790  # TCP file transfer from client to master
PORT_TCP_MASTERSERVER = 8791  # TCP file transfer from master to each process

### SDFS configuration ###
SDFS_DIRECTORY = "sdfs"

DEFAULT_NUM_REPLICAS = 4
DEFAULT_MAX_VERSIONS = 5

PUT_TIMEOUT = 5
FILE_CHUNK_SIZE = 32 * 1024 * 1024
GET_RETRY = 3

### FD configuration ###
DEFAULT_NUM_NEIGHBORS = 4

### Miscellaneous configuration ###
LOGLEVEL = 10
