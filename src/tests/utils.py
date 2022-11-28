import socket
from threading import Lock
from typing import TYPE_CHECKING, Tuple
from queue import Queue, Empty
from dataclasses import dataclass
from typing import Dict, List

from src.utils import _Address, _Port, getLogger

if TYPE_CHECKING:
    from src.utils import Message

logger = getLogger(__name__, "test_socket.log")

####### The following code is retrieved from kindall's answer in StackOverflow post:
### https://stackoverflow.com/a/6894023
from threading import Thread
class TestThread(Thread):
    
    def __init__(self, group=None, target=None, name=None,
                 args=(), kwargs={}, Verbose=None):
        Thread.__init__(self, group, target, name, args, kwargs)
        self._return = None

    def run(self):
        if self._target is not None:
            self._return = self._target(*self._args, **self._kwargs)
    def join(self, *args):
        Thread.join(self, *args)
        return self._return
####### End quote #######


####### The following code is retrieved from Thomas Grainger's answer in StackOverflow post:
### https://stackoverflow.com/questions/40447290/python-unittest-and-multithreading
import threading

class catch_threading_exception:
    """
    https://docs.python.org/3/library/test.html#test.support.catch_threading_exception
    Context manager catching threading.Thread exception using
    threading.excepthook.

    Attributes set when an exception is catched:

    * exc_type
    * exc_value
    * exc_traceback
    * thread

    See threading.excepthook() documentation for these attributes.

    These attributes are deleted at the context manager exit.

    Usage:

        with support.catch_threading_exception() as cm:
            # code spawning a thread which raises an exception
            ...

            # check the thread exception, use cm attributes:
            # exc_type, exc_value, exc_traceback, thread
            ...

        # exc_type, exc_value, exc_traceback, thread attributes of cm no longer
        # exists at this point
        # (to avoid reference cycles)
    """

    def __init__(self):
        self.exc_type = None
        self.exc_value = None
        self.exc_traceback = None
        self.thread = None
        self._old_hook = None

    def _hook(self, args):
        self.exc_type = args.exc_type
        self.exc_value = args.exc_value
        self.exc_traceback = args.exc_traceback
        self.thread = args.thread

    def __enter__(self):
        self._old_hook = threading.excepthook
        threading.excepthook = self._hook
        return self

    def __exit__(self, *exc_info):
        threading.excepthook = self._old_hook
        del self.exc_type
        del self.exc_value
        del self.exc_traceback
        del self.thread
####### End quote #######


@dataclass
class TestSocketSettings:
    drop_rate: int = 0


class TestSocketDaemon:

    # Stores a mapping of messages sent
    fd: Dict[_Address, Dict[_Port, Queue]] = {}
    # {host: {port1: Queue(UDPMessage, ...), port2: , ...}, host2: { }, ...}

    # Lock for self.fd
    _fd_lock = Lock()

    # Store a copy of all packets
    packets: List["UDPacket"] = []

    # Settings to emulate socket instabilities
    settings = TestSocketSettings()


@dataclass
class UDPacket:

    data: bytes
    offset: int = 0
    addr: _Address = None

    # Debug fields
    sent: bool = True


class TestSocket(TestSocketDaemon):
    def __init__(self, host, port) -> None:
        self.host = host
        self.port = port
        self.addr = (self.host, self.port)

        self.bind_host = None
        self.bind_port = None

        # Used for testing purposes
        self.last_packet: bytes = None
        self.last_packet_addr = None
        self.sendto_cnt = 0

        self.next_timeout = None

        self.__closed = False

    def __enter__(self):
        return self

    def __exit__(self):
        self.close()

    @property
    def closed(self):
        return (
            self.__closed  # s.close() has been called
            or (self.bind_host is None and self.bind_port is None)  # not yet binded
            or (self.bind_host in self.fd and self.bind_port not in self.fd[self.bind_host])  # binded but deleted
        )

    def sendto(self, __data: bytes, __address: tuple):
        self.last_packet = __data
        self.last_packet_addr = __address
        self.sendto_cnt += 1

        if len(__address) != 2:
            raise ValueError(f"Invalid input __address: {__address}")
        to_host, to_port = __address

        self._fd_lock.acquire(timeout=5)

        packet = UDPacket(__data, addr=self.addr)
        if to_host in self.fd and to_port in self.fd[to_host]:
            self.fd[to_host][to_port].put(packet, block=False)  # raise error if maxsize reached
            logger.info(f"{(self.host, self.port)} sendto {__address}: sent")
        else:
            packet.sent = False
            logger.info(f"{(self.host, self.port)} sendto {__address}: dropped")
        self.packets.append(packet)
        self._fd_lock.release()

    def bind(self, __address: _Address) -> None:
        bind_host, bind_port = __address
        self._fd_lock.acquire(timeout=5)
        if bind_host in self.fd and bind_port in self.fd[bind_host]:
            # Multiple bind on the same address is forbidded
            self._fd_lock.release()
            raise ValueError(f"Address already binded: {__address}")

        # Start listening on host, port
        if bind_host == "":
            bind_host = self.host
        else:
            self.host = bind_host

        if bind_host not in self.fd:
            self.fd[bind_host] = {}
        if bind_port not in self.fd[bind_host]:
            self.fd[bind_host][bind_port] = Queue(64)
        self._fd_lock.release()

        self.bind_host = bind_host
        self.bind_port = bind_port

    def recvfrom(self, __bufsize: int) -> Tuple[bytes, _Address]:
        if self.bind_port is None or self.bind_host is None:
            raise ValueError("No address binded for recv...")

        # If already binded to an address, the Queue should be ready
        try:
            packet: UDPacket = self.fd[self.bind_host][self.bind_port].get(
                block=True, timeout=self.next_timeout
            )  # block until the queue is ready
        except Empty:
            raise socket.timeout()
        finally:
            self.next_timeout = None
        return (packet.data, packet.addr)

    def settimeout(self, __value: float) -> None:
        self.next_timeout = __value

    def setsockopt(self, *args):
        return

    def close(self):
        # Clear everything sent to (bind_host, bind_port)
        self._fd_lock.acquire(timeout=5)
        if (
            self.bind_host is not None
            and self.bind_host in self.fd
            and self.bind_port in self.fd[self.bind_host]
        ):
            del self.fd[self.bind_host][self.bind_port]

        self.__closed = True
        logger.info(f"{(self.host, self.port)} called s.close()")

        self._fd_lock.release()
