import hashlib
import logging
import os
import pickle
from socket import gethostname
from typing import Optional, Any

from src.config import LOGLEVEL

TIME_FORMAT_STRING = "%H:%M:%S"

LOGFORMATTER = logging.Formatter(
    "%(asctime)s %(levelname)s %(name)s@%(lineno)d: %(message)s"
)

### Typings definition ###
from typing import Tuple

_Host = str
_Port = int
_Address = Tuple[_Host, _Port]
### End typings def ###


class Message:
    """Communication protocol among servers.

    Example:
        Server with id 3 finds that server 5 left:
        msg = Message(3, ..., ..., ..., "LEAVE", content={"id": 5})

        Server with id 4 PING server 5:
        msg = Message(4, ..., ..., ..., "PING", ...)
        socket.sendto(msg, (host_5, port_5))

        Server with id 5 ACK server 4's ping:
        msg = Message(5, ..., ..., ..., "ACK", ...)

        Server with id 6 joins:
        msg = Message(6, ..., ..., ..., "JOIN", ...)
        
        Server with id 4 finds that server 6 joins:
        msg = Message(6, ..., ..., ..., "JOIN", ...)
    
    """
    def __init__(self, id: int, host: str, port, time_stamp: float, message_type: str, content=None) -> None:
        """
        :attr id: unique identifier for each process
        :attr host: host name
        :attr port: port for the message
        :attr time_stamp: message epoch timestamp
        :attr message_type: JOIN | LEAVE | PING | ACK
        :attr content: Any useful payload for the message
        """
        self.id: int = id
        self.host: str = host
        self.port: int = port
        self.time_stamp: float = time_stamp
        self.message_type: str = message_type
        self.content: Any = content

    @property
    def mid(self):
        """Distinguishes one Message from another."""
        if isinstance(self.content, int):
            content_set = self.content
        else:
            content_set = dict(sorted(self.content.items())) if self.content is not None else None
        return hashlib.sha1(pickle.dumps(
            (self.message_type, content_set)
            )).hexdigest()

def get_host() -> str:
    """Defines how components get their host name."""
    host = gethostname()
    if host == "localhost.localdomain":
        host = "fa22-cs425-1303.cs.illinois.edu"  # special for 1303 vm
    return host

def getLogger(name: str, filename: Optional[str] = ..., level: int = ...):
    """Usage: logger = getLogger(__name__)"""
    logger = logging.getLogger(name)

    if level is Ellipsis:
        level = LOGLEVEL
    logger.setLevel(level)

    if filename is Ellipsis:
        # Default file name
        hname = get_host()
        N_VM = name = int(hname.split(".")[0][-2:])  # "fa22-cs425-13xx.cs.illinois.edu" => xx
        filename = f"vm{N_VM}.log"
    # Default path: src/vm*.log
    path = os.path.realpath(os.path.dirname(__file__))
    os.chmod(path, 0o777)
    filename = os.path.realpath(os.path.join(os.path.dirname(__file__), filename))

    file_handler = logging.FileHandler(filename, mode="w")
    file_handler.setLevel(level)
    file_handler.setFormatter(LOGFORMATTER)
    logger.addHandler(file_handler)

    logger.propagate = False

    return logger


def socket_should_stop(data) -> bool:
    # Receives stop message from controller
    return data is not None and data == b"stop"
        
