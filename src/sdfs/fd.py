"""Group Membership List service for distributed systems.

Credits: Peiran Wang, Hanbo Guo
"""


import threading
import socket
import time
import pickle
from collections import OrderedDict
from dataclasses import dataclass, field
from typing import Any, List, Dict, Tuple, Union, TYPE_CHECKING

from src.config import *
from .utils import getLogger, Message, get_host, socket_should_stop

import sys


if TYPE_CHECKING:
    from src.sdfs import FileTable


RUNNING = 0
SUSPECT = 100
LEAVED = -1

PING_TIMEOUT = 2

MSG_CACHE_SIZE = 20

DELETE_TIMEOUT = 3
DELETE_TIME_INTERVAL = 0.5


class _MsgCache:
    """Record JOIN/LEAVE messages sent by self."""

    def __init__(self, size: int = ...) -> None:
        self.max_size = size
        if size is Ellipsis:
            self.max_size = MSG_CACHE_SIZE
        self._content: Dict[str, list] = OrderedDict()  # {mid: [1, 2, ...], ...}
        # OrderedDict is necessary for correct poping

    def __contains__(self, mid: str) -> bool:
        return mid in self._content

    def set(self, mid: str, to_id: int) -> None:
        """Sets message with ``mid`` has been sent to ``to_id``.
        If ``self.max_size`` has reached, pop first element (earliest)."""
        if self._content.get(mid) is None:
            self._content[mid] = [to_id]
        elif to_id not in self._content[mid]:
            self._content[mid].append(to_id)
        if len(self._content) > self.max_size:
            self._content.popitem(last=False)

    def sent(self, mid: str, to_id: int) -> bool:
        """Whether message with ``mid`` has been sent to ``to_id``."""
        if mid not in self._content:
            return False
        return to_id in self._content[mid]


@dataclass
class _MLEntry:

    id: int
    host: str
    port: int
    timestamp: float  # last known alive time
    state: int = RUNNING

    def __eq__(self, __o: object) -> bool:
        return (
            self.id == __o.id
            and self.host == __o.host
            and self.port == __o.port
            and self.timestamp == __o.timestamp
        )

    def __lt__(self, __o: object) -> bool:
        return self.id < __o.id

    def __str__(self) -> str:
        return f"id: {self.id}\thost: {self.host}\tport: {self.port}\ttimestamp: {self.timestamp}\tstate: {self.state}"

    def __getitem__(self, key: str) -> Any:
        return getattr(self, key)

    def __setitem__(self, key: str, value: Any) -> None:
        setattr(self, key, value)

    def get(self, key: str, default=None) -> Any:
        return getattr(self, key, default)


@dataclass
class MembershipList:
    """A dictionary-like membership list.

    A list is used internally, but a dictionary interface is also provided.
    Use ml[key] and ml.get is recommended.
    """

    # Python 3.6 dictionary does not preserve insert order,
    # so it could be dangerous...
    _content: List = field(default_factory=list)

    def __len__(self):
        return len(self._content)

    def __iter__(self):
        self._content.sort()
        yield from self._content

    def __str__(self) -> str:
        return "\n".join([str(entry) for entry in self.content])

    def get(self, key: str, default=None) -> "_MLEntry":
        for entry in self._content:
            if entry.id == int(key):
                return entry
        return default

    def __getitem__(self, key: str) -> "_MLEntry":
        for entry in self._content:
            if entry.id == int(key):
                return entry
        raise KeyError(f"No node with id {key}.")

    def __setitem__(self, key: str, value: "_MLEntry") -> None:
        target: "_MLEntry" = None
        for entry in self._content:
            if entry.id == int(key):
                target = entry
                break
        if target is None:
            self._content.append(value)
        else:
            target.host = value.host
            target.port = value.port
            target.timestamp = value.timestamp

    def __contains__(self, id: str) -> bool:
        for entry in self._content:
            if entry.id == int(id):
                return True
        return False

    def append(self, __value: "_MLEntry") -> None:
        """Appends an ``_MLEntry`` into this membership list.

        Does not check if ``__value`` exists in ML or not."""
        if not isinstance(__value, _MLEntry):
            raise ValueError(f"Should be an _MLEntry instance, not {type(__value)}")
        self._content.append(__value)

    def pop(self, key: str, default=None) -> "_MLEntry":
        idx = 0
        found = False
        for i in range(len(self.content)):
            if self.content[i].id == int(key):
                idx = i
                found = True
                break
        if found:
            return self._content.pop(idx)
        else:
            return default

    def clear(self) -> None:
        self._content = []

    def neighbors(self, id: str, n: int = ..., include_self: bool = False) -> List[_MLEntry]:
        """Gets ``n`` neighbors of ``id`` from ml.
        If ``include_self = True``, returns ``n`` neighbors with ``id`` in it. Default ``False``.
        """
        l = len(self._content)
        ret = []
        idx = self.__index(int(id))
        for pos in set([((idx + i) % l) for i in [-2, -1, 1 - int(include_self), 2 - int(include_self)]]):
            if pos != idx:
                ret.append(self.content[pos])
        if include_self:
            ret.append(self.__getitem__(id))
        return ret

    def contains(self, value: Any, __type: str = "id") -> bool:
        """Usage: ml.contains(value"fa22-cs...", __type="host"). Can't compare timestamp now."""
        __type_accepted = ["id", "host", "port", "timestamp"]
        if __type not in __type_accepted:
            raise ValueError(f"{__type} is invalid. Use one of {__type_accepted}.")
        for entry in self.content:
            attr = getattr(entry, __type)
            if attr == value:
                return True
            elif isinstance(attr, int) or isinstance(value, int) and int(attr) == int(value):
                return True
        return False

    def __index(self, key: str) -> int:
        """Index of ``key`` in the sorted ``self.content``."""
        ret = 0
        for entry in self.content:
            if entry.id == int(key):
                return ret
            ret += 1
        raise ValueError(f"{key} is not in ML")

    @property
    def content(self) -> List["_MLEntry"]:
        self._content.sort()
        return self._content


class FailureDetector:
    def __init__(self, id, host: str = ..., port: int = ...) -> None:
        self.id = id
        self.host = host
        if host is Ellipsis:
            self.host = get_host()
        self.port = port
        if port is Ellipsis:
            self.port = PORT_FDSERVER

        self.ml: MembershipList = MembershipList()
        self.__msg_cache = _MsgCache()
        self.delete_pool = {}
        self.ack_cache = {}

        self.ml_lock = threading.Lock()
        self.ack_cache_lock = threading.Lock()
        self.delete_pool_lock = threading.Lock()

        self.logger = getLogger(__name__)

        self.state = LEAVED
        
        self.coordinator = ""
        self.coordinator_id = -1

    """
    Get neighboors:
        1. Sort membership list by time
        2. Neighboors are those who joined group after current sever or before(since it is a ring)
    """

    @property
    def neighbors(self) -> List[_MLEntry]:
        return self.ml.neighbors(self.id)

    # Check if message with mid has been sent to the process with toid
    def sent(self, mid: str, to: int) -> bool:
        return self.__msg_cache.sent(mid, to)

    def set_message_sent(self, mid: str, to: int) -> bool:
        self.__msg_cache.set(mid, to)

    def generate_message(self, m_type: str, content: Any = None) -> Message:
        if content == None:
            content = {"time_stamp": time.time()}
        return Message(self.id, self.host, self.port, time.time(), m_type, content)

    ### Utility functions ###

    def sendto_dns(self, message: Message, s: socket.socket):
        s.sendto(pickle.dumps(message), (DNS_SERVER_HOST, DNS_SERVER_PORT))

    def multicast(self, message: Message, s: socket.socket, sent_check: bool = True, who: str = "Receiver"):
        """Multicast ``message`` to neighbors via socket ``s`` if haven't done so.
        If needs to avoid cyclic messages (a->b->c->a->b...), set ``sent_check = True``."""
        self.ml_lock.acquire()
        neighbors = self.neighbors[:]

        for neighbor in neighbors:
            # If message not sent and target is not the sender of the message
            if (not sent_check) or not self.sent(message.mid, neighbor.id):
                self.send_to(message, neighbor.id, s, sent_check, who)
            else:
                self.logger.info(
                    f"Host {self.id} - {who} NOT send {message.message_type} to neighbor {neighbor.id}"
                )
        self.ml_lock.release()

    def send_to(
        self,
        message: Message,
        target_id: int,
        s: socket.socket,
        set_sent: bool = True,
        who: str = "Receiver",
    ):
        """Sends ``message`` to ``addr`` via socket ``s``."""
        addr = (self.ml[target_id].host, self.ml[target_id].port)
        s.sendto(pickle.dumps(message), addr)
        if set_sent:
            self.set_message_sent(message.mid, target_id)
        self.logger.info(f"Host {self.id} - {who} send {message.message_type} to neighbor {target_id}")

    def __log_start_processing_message(self, message: Message):
        self.logger.debug(f"Host {self.id}: Starts processing {message.message_type} from {message.id}")

    def __log_ml_entry_update(
        self,
        updated_entry_id: int,
        update_what: str,
        update_to: Union[int, str, float],
        message: Message,
        who: str = "Receiver",
    ):
        """Logs any updats on an entry in ML, such as updating state to ALIVE, changing timestamp, etc.

        Args:
            updated_entry_id: int
                ML entry ID. The entry with this id has been updated.
            update_what: str
                What field is updated. Possible choices include: timestamp, state, etc.
                Needs to be the same as _MLEntry parameters.
            update_to: int | str | float
                New value. The ML entry with id = ``update_entry_id`` has updated field ``update_what`` to value ``update_to``.
            message: Message
                Upon receiving this message, the program decides to update ML.
            who: str
                Who issued this update. Could be "Receiver", "Checker", etc.
        """
        self.logger.info(
            f"Host {self.id}: {message.message_type} from {message.id} => MLENTRY UPDATE: {who} updates entry {updated_entry_id}'s {update_what} from {getattr(message, update_what)} to {update_to}."
        )

    def __log_ml_update(self, new_entry_id: int, update_how: str, message: Message, who: str = "Receiver"):
        """Logs any direct update on ML, such as appending a new ``_MLEntry``, or poping an entry."""
        self.logger.info(
            f"Host {self.id}: {message.message_type} from {message.id} => ML {update_how}: {who} appends entry {new_entry_id}."
        )

    """
    Server would receive following messages:
        1. JOIN -- A new server have been joined into group via introducer
        2. LEAVE -- A server have leaved or be detected as fault
        3. PING -- Be sent by other servers to check if current server is alive
        4. ACK -- Current server's neighboor is alive 
    """

    def receiver(self, s: socket.socket = ...):
        if s is Ellipsis:
            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind((self.host, self.port))

        while True:
            try:
                self.ml_lock.acquire()
                if self.state == LEAVED or self.id not in self.ml:
                    self.ml_lock.release()
                    continue
                self.ml_lock.release()

                data, _ = s.recvfrom(4096)
                if data:

                    if socket_should_stop(data):
                        break

                    message: Message = pickle.loads(data)

                    # If received the message from a server, then the server must be alived
                    message_sender = message.id
                    self.ml_lock.acquire()
                    if message_sender in self.ml and self.ml[message_sender].state == SUSPECT:
                        self.ml[message_sender].state = RUNNING
                    self.ml_lock.release()
                    self.delete_pool_remove(message_sender)

                    if message.message_type == "JOIN":
                        # Forward to neightbors, update self.ml
                        self.__log_start_processing_message(message)

                        join_message: Message = self.generate_message("JOIN", content=message.content)

                        # Multicast to neighbors if haven't done so
                        self.multicast(join_message, s)

                        # Add sender's info to ML
                        self.ml_lock.acquire()
                        entry = _MLEntry(
                            message.content["id"],
                            message.content["host"],
                            message.content["port"],
                            message.content["time_stamp"],
                        )
                        self.ml[message.content["id"]] = entry
                        self.__log_ml_update(message.content["id"], "APPEND", message)
                        self.ml_lock.release()

                    elif message.message_type == "LEAVE":
                        self.__log_start_processing_message(message)

                        leave_message = self.generate_message("LEAVE", content=message.content)

                        self.multicast(leave_message, s)  # tell neighbor

                        #If I received the message that I am LEAVE, it is miskilling.
                        if message.content["id"] == self.id:
                            #Then I kill myself
                            sys.exit(0)

                        # Remove from ML if possible
                        self.ml_lock.acquire()
                        if message.content["id"] in self.ml:
                            self.ml.pop(message.content["id"], None)
                            
                            #send FAILURE message to sdfs by TCP 
                            failure_message = self.generate_message(
                                "FAILURE", content={"id":message.content["id"], "time_stamp": time.time()}
                            )
                            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s1:
                                s1.connect((self.host, PORT_SDFS_GETFAILURE))
                                s1.sendall(pickle.dumps(failure_message))
                                s1.shutdown(socket.SHUT_WR)
                            
                            #if coordinator leave, send FAILURE message to worker by TCP
                            if message.content["id"] == self.coordinator_id:
                                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s3:
                                    s3.connect((self.host, PORT_WORKER_FAILURE_LISTEN))
                                    s3.sendall(pickle.dumps(failure_message))
                                    s3.shutdown(socket.SHUT_WR)
                            
                            self.__log_ml_update(message.content["id"], "POP", message)
                
                        self.ml_lock.release()

                    else:
                        self.logger.warn(f"Invalid message typ: {message.message_type}")

            except Exception as e:
                self.logger.critical(e)
                s.close()
                raise e from None
        s.close()

    def ping_ack_handler(self):
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
            s.bind(("", PORT_FDPING))
            while True:
                try:
                    data, _ = s.recvfrom(4096)
                    if data:
                        message: Message = pickle.loads(data)
                        if message.message_type == "PING":
                            ack_message = self.generate_message("ACK")
                            s.sendto(pickle.dumps(ack_message), (message.host, PORT_FDPING))
                        elif message.message_type == "ACK":
                            self.ack_cache_lock.acquire()
                            self.ack_cache[self.id].add(message.id)
                            self.ack_cache_lock.release()

                        self.ml_lock.acquire()
                        if message.id in self.ml and self.ml[message.id].state == SUSPECT:
                            self.ml[message.id].state = RUNNING
                            self.logger.info(
                                f"Host {self.id}: received {message.message_type} from {message.id}, so I removed it from delete_pool"
                            )
                            self.delete_pool_remove(message.id)
                        self.ml_lock.release()

                except Exception as e:
                    raise e

    def ping_thread(self):
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
            while True:
                self.ml_lock.acquire()
                if self.id not in self.ml:
                    self.ml_lock.release()
                    continue
                neighbors = self.neighbors[:]
                self.ml_lock.release()

                ping_message = self.generate_message("PING")

                self.ack_cache_lock.acquire()
                self.ack_cache[self.id] = set()
                self.ack_cache_lock.release()

                # ids = self.multicast(s, ping_message, PORT_FDPING)
                self.ml_lock.acquire()
                for neighbor in self.neighbors:
                    s.sendto(pickle.dumps(ping_message), (neighbor.host, PORT_FDPING))
                self.ml_lock.release()

                time.sleep(PING_TIMEOUT)

                # check whether receive acks from neighbors
                self.ack_cache_lock.acquire()
                ack_set = self.ack_cache[self.id]
                self.ack_cache.pop(self.id)
                self.ack_cache_lock.release()

                ids = [i.id for i in neighbors]

                for id in ids:
                    # did not recieved id's ack
                    if id not in ack_set and id in self.ml and self.ml[id].state == RUNNING:
                        curr = time.time()
                        self.logger.info(f"Host {self.id}: no ACK from {id} at time {curr}")
                        self.delete_pool_add(id, time.time())
                        self.ml_lock.acquire()
                        self.ml[id].state = SUSPECT
                        self.ml_lock.release()

    def join(self, s: socket.socket = ...) -> Tuple[int, "FileTable"]:
        """Defines how a new server joins the cluster.

        The server needs to:
        1. Send ``GET_INTRODUCER`` message to DNS server
        2. Receives response from DNS server, and get ``introducer_host``, ``introducer_port``, and assigned global id
        3. Send ``JOIN`` message to the introducer via the host, port provided by DNS
        4. Receive a membership list from the introducer
        4. Mark itself as in ``RUNNING`` state
        5. Multicast a different ``JOIN`` message to neighbors in the membership list

        Messages:
        1. ``GET_INTRODUCER`` self -> DNS
        2. ``RESP INTRODUCER`` DNS -> self
        3. ``JOIN`` self -> introducer
        4. ``UPDATE`` introducer -> self
        5. ``JOIN`` self -> neighbor(s)
        """
        if s is Ellipsis:
            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

        s.bind((self.host, PORT_FDUPDATE))

        message = self.generate_message("GET_INTRODUCER")

        # Get introducer host / port
        s.sendto(pickle.dumps(message), (DNS_SERVER_HOST, DNS_SERVER_PORT))
        s.settimeout(3)
        try:
            data, _ = s.recvfrom(4096)
        except socket.timeout:
            print("[ERR] DNS is down")
            s.close()
            return -1, None

        if not data:
            raise ValueError("No response from DNS...")

        message: Message = pickle.loads(data)
        introducer_host = message.content["introducer_host"]
        introducer_port = message.content["introducer_port"]
        self.id = message.content["assigned_id"]
        self.coordinator = message.content["coordinator_host"]
        self.coordinator_id = message.content["coordinator_id"]

        # send JOIN to introducer
        message = self.generate_message("JOIN")
        s.sendto(pickle.dumps(message), (introducer_host, introducer_port))
        print(f"FD: Sent JOIN to introducer {introducer_host}:{introducer_port}")

        # listen to UPDATE port and get membership and id
        try:
            s.settimeout(2)
            data, _ = s.recvfrom(4096)
            if data:
                update_message: Message = pickle.loads(data)
                self.ml = update_message.content["ML"]

                curr = time.time()
                join_message = self.generate_message(
                    "JOIN",
                    content={"id": self.id, "host": self.host, "port": self.port, "time_stamp": curr},
                )

                self.ml_lock.acquire()
                if self.id not in self.ml:
                    self.ml.append(_MLEntry(self.id, self.host, self.port, curr))  # add self to ml
                    self.__log_ml_update(self.id, "APPEND", update_message, "Join")
                self.ml_lock.release()

                # Multicast JOIN to neighbors
                self.multicast(join_message, s, who="Join")
                self.state = RUNNING
                print("-------------")
                print(self.ml)
                print("-------------")
        except socket.timeout:
            print("[ERR] Introducer is down")
            return self.id, None
        finally:
            s.close()
        return self.id, update_message.content["FT"]

    def leave(self):
        """A server want to leave from group, send message to its neighboor."""
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
            self.state = LEAVED
            message = self.generate_message("LEAVE", {"id": self.id})
            self.ml_lock.acquire()
            for i in self.neighbors:
                s.sendto(pickle.dumps(message), (i.host, i.port))
            self.ml_lock.release()
            self.sendto_dns(message, s)
        self.ml = MembershipList()

    def delete_pool_checker(self):
        while True:
            time.sleep(DELETE_TIME_INTERVAL)
            curr_time = time.time()
            self.delete_pool_lock.acquire()

            if len(self.delete_pool) <= 0:
                self.delete_pool_lock.release()
                continue
            self.delete_pool_lock.release()

            for id, time_stamp in self.delete_pool.copy().items():
                if curr_time - time_stamp > DELETE_TIMEOUT:
                    self.logger.info(f"Host {self.id}: delete pool checker finds id = {id} timeout")
                    self.ml_lock.acquire()
                    if id in self.ml and self.ml[id].state == RUNNING:
                        self.delete_pool_remove(id)
                        self.ml_lock.release()
                        continue
                    if id not in self.ml:
                        #has been removed by multicast LEAVE message
                        self.ml_lock.release()
                        continue
                    else:
                        self.ml.pop(id)
                        self.ml_lock.release()
                        
                        leave_message = self.generate_message(
                            "LEAVE", content={"id": id, "time_stamp": time_stamp}
                        )
                        
                        self.logger.info(f"Host {self.id}: checker decides to delete {id} at time {curr_time}")
                        self.delete_pool_remove(id)
                        self.logger.info(f"Host {self.id}: current delete pool: {self.delete_pool}")
                       
                        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
                            self.__log_ml_update(id, "POP", leave_message, who="delete_pool_checker")
                            self.multicast(leave_message, s, sent_check=False, who="delete_pool_checker")
                            #send to DNS to change the introducer
                            s.sendto(pickle.dumps(leave_message), (DNS_SERVER_HOST, DNS_SERVER_PORT))
                            
                            #send FAILURE message to sdfs by TCP 
                            failure_message = self.generate_message(
                                "FAILURE", content={"id": id, "time_stamp": time_stamp}
                            )
                            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s1:
                                s1.connect((self.host, PORT_SDFS_GETFAILURE))
                                s1.sendall(pickle.dumps(failure_message))
                                s1.shutdown(socket.SHUT_WR)
                            
                            #send FAILURE message to coordinator by TCP if it is not the coordinator's failure
                            #otherwise send FAILURE message to worker by TCP if it is the coordinator's failure
                            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s2:
                                print(f"id:{id} vs. {self.coordinator_id} vs. coordinator_host: {self.coordinator}")
                                if id != self.coordinator_id:
                                    s2.connect((self.coordinator, PORT_COORDINATOR_FAILURE_LISTEN))
                                else:
                                    s2.connect((self.host, PORT_WORKER_FAILURE_LISTEN))
                                s2.sendall(pickle.dumps(failure_message))
                                s2.shutdown(socket.SHUT_WR)
                            

    def delete_pool_remove(self, id):
        self.delete_pool_lock.acquire()
        if id in self.delete_pool:
            self.delete_pool.pop(id)
            self.logger.info(f"Host {self.id}: delete pool pops id = {id}")
        self.delete_pool_lock.release()

    def delete_pool_add(self, id, timestamp):
        self.delete_pool_lock.acquire()
        if id not in self.delete_pool:
            self.delete_pool[id] = timestamp
            self.logger.info(f"Host {self.id}: delete pool adds id = {id}")
        self.delete_pool_lock.release()

    def run(self):
        receiveWorker = threading.Thread(target=self.receiver)
        ping_thread = threading.Thread(target=self.ping_thread)
        ping_ack_handler = threading.Thread(target=self.ping_ack_handler)
        delete_pool_checker = threading.Thread(target=self.delete_pool_checker)

        return [receiveWorker, ping_thread, ping_ack_handler, delete_pool_checker]


if __name__ == "__main__":
    fd = FailureDetector(0)
    fd.run()
