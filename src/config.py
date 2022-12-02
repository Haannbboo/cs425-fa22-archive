### Ports ###
PORT_SDFS = 8765  # SDFS receiver/sender
PORT_FDPING = 8766  # Failure detector ping/ack
PORT_FDSERVER = 8767  # Failure detector receiver
PORT_FDUPDATE = 9000  # Failure detector update port
PORT_FDINTRODUCER = 8887  # Introducer port
PORT_SDFS_GETFAILURE = 9001 #where sdfs get failure message

DNS_SERVER_HOST = "fa22-cs425-1310.cs.illinois.edu"
DNS_SERVER_PORT = 8769

### Idunno configuration ##
PORT_IDUNNO_CLIENT = 10086  # coordinator uses this to contact client
PORT_IDUNNO_COORDINATOR = 10087  # client use this to contact coordinator
PORT_REQUEST_JOB = 10234  # worker request job from this
PORT_COMPLETE_JOB = 10235  # worker report job completion
PORT_START_WORKING = 10300  # worker listens to this port to know if they can be lazy
PORT_STANDBY_UPDATE = 10310  # communication between coordinator & standby
PRE_TRAIN_PORT = 10236

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
