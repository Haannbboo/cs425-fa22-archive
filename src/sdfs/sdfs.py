from dataclasses import dataclass, field
import os
import pickle
import socket
import threading
import time
from typing import Any, List, Set, Dict, Tuple, TYPE_CHECKING
import random

from ..config import *
from .fd import FailureDetector
from .utils import get_host, Message, socket_should_stop


if TYPE_CHECKING:
    from fd import _MLEntry

CHECK_INTERVAL = 0.5
DELETE_TIMEOUT = 2.5


class FileTable:
    """Keeps track of what and where files are stored locally and remotely.

    Under the ``SDFS_DIRECTORY`` directory, a file might have multiple versions (<= DEFAULT_MAX_VERSIONS).
    A client only knows about the sdfs file name, such as ``abc.txt``,
    but there might be ``abc.txt.1`` and ``abc.txt.2`` under ``SDFS_DIRECTORY`` directory.
    """

    def __init__(self):
        self.files: Dict[List[SDFSFile]] = {}  # fname: [SDFSFile version 1, SDFSFile version 2, ..., SDFSFile version 5]
        self.processes: Dict[set] = {}  # id: set(fname)

    def __contains__(self, fname: str) -> bool:
        """``fname`` in FileTable"""
        return fname in self.files

    def insert(self, remote: str, targets: set, version: int) -> "SDFSFile":
        """Inserts filename ``remote`` with ``version`` to processes with id in ``targets``.
        Returns a SDFSFile that needs to be deleted locally, or None if no local deletion needed."""
        for target in targets:
            if target not in self.processes:
                self.processes[target] = set()
            self.processes[target].add(remote)
        if remote not in self.files:
            self.files[remote] = []

        ret: "SDFSFile" = None
        # If ``version`` already exists, update SDFSFile with ``version``
        for file in self.files[remote]:
            if file.version == version:
                file.replicas = targets
                return ret

        # If no ``version`` exists for ``remote``, append
        if len(self.files[remote]) == DEFAULT_MAX_VERSIONS:
            # Pop the earliest version if DEFAULT_MAX_VERSIONS reached.
            self.files[remote].sort(key=lambda file: file.version)
            ret = self.files[remote].pop(0)

        new_version = SDFSFile(self._remote_to_sdfsfname(remote, version), version, targets)
        self.files[remote].append(new_version)
        self.files[remote].sort(key=lambda file: file.version)
        return ret

    def get(self, fname: str, version: int = -1) -> "SDFSFile":
        """Gets a reference to ``SDFSFile`` with ``version`` and ``fname``. Default newest version."""
        file = self.files.get(fname)
        if file is None:
            return None
        if version == -1:  # newest version
            return file[-1]
        else:  # specified version
            for version_file in file:
                if version_file.version == version:
                    return version_file
        return None

    def delete(self, remote: str):
        """Deltes filename ``remote`` from this FileTable."""
        replicas = []
        if remote in self.files:
            for file_version in self.files[remote]:
                replicas.extend(file_version.replicas)
            del self.files[remote]  # remove remote:[file]
        for process in replicas:
            self.processes[process].discard(remote)

    def print(self, what: str, **kwd):
        """Pretty print ``what``, one of ["store", "ls"]."""
        if what == "store":
            if "id" not in kwd:
                raise ValueError("FileTable needs 'id' to print 'store'.")
            print("Name\tVersion\t\tReplicas\tContent Length")
            print("----\t-------\t\t--------\t--------------")
            if kwd["id"] in self.processes:
                for fname in self.processes[kwd["id"]]:
                    for file_version in self.files[fname]:
                        print(file_version)
        elif what == "ls":
            if kwd["fname"] in self.files:
                print("Name\tVersion\t\tReplicas\tContent Length")
                print("----\t-------\t\t--------\t--------------")
                for file_version in self.files[kwd["fname"]]:
                    print(file_version)

    def update(self, filename: str, versionInfo, leaved_id, new=-1):
        if new == -1:
            if leaved_id in self.processes:
                self.processes[leaved_id] = set()
            self.files[filename] = versionInfo

        # the case that we need to recover the replica
        else:
            self.files[filename] = versionInfo
            if new in self.processes:
                self.processes[new].add(filename)
            else:
                self.processes[new] = set([filename])

    def clear(self):
        """Clears content of this FileTable."""
        self.files = {}
        self.processes = {}

    def _remote_to_sdfsfname(self, remote: str, version: int) -> str:
        """Defines how to construct sdfsfname with a given ``version`` of ``remote``."""
        filename, file_extension = os.path.splitext(remote)
        fn = filename + "_" + str(version) + file_extension
        return fn


@dataclass
class SDFSFile:
    fname: str
    version: int = 0  # version of this file
    replicas: Set[int] = field(default_factory=set)  # where the recent version is stored
    content = None  # cache content of most recent version, not stored in disk

    def __str__(self):
        l_content = len(self.content) if self.content is not None else 0
        return f"{self.fname}\t{self.version}\t\t{self.replicas}\t\t{l_content}"


class SDFS:
    def __init__(self, host: str = ..., port: int = ...):
        self.id = 0
        self.host = host
        if host is Ellipsis:
            self.host = get_host()
        self.port = port
        if port is Ellipsis:
            self.port = PORT_SDFS

        self.fd = FailureDetector(self.id, self.host)
        self.ft = FileTable()

        self.delete_pool = set()
        self.put_ack = {}

        self.ft_lock = threading.Lock()
        self.delete_pool_lock = threading.Lock()
        self.put_ack_lock = threading.Lock()
        
        self.dir = os.path.join(os.getcwd(), SDFS_DIRECTORY)
        if not os.path.isdir(self.dir):
            os.makedirs(self.dir)

        ### Test flags ###
        self._started = False

    @property
    def W(self):
        # decide write consistency level W
        return min(int(DEFAULT_NUM_REPLICAS / 2) + 1, len(self.all_processes))

    @property
    def R(self):
        # decide read consistency
        return min(int(DEFAULT_NUM_REPLICAS / 2) + 1, len(self.all_processes))

    @property
    def all_processes(self) -> List["_MLEntry"]:
        return self.fd.ml.content

    @property
    def all_processes_ids(self) -> List[int]:
        return [process.id for process in self.all_processes]

    @property
    def is_master(self):
        # Lowest id node is master
        # Since ML converges within few seconds,
        # just take the lowest id node in ML as the leader.
        return self.all_processes[0].id == self.id

    @property
    def master(self) -> str:
        """Gets master hostname of this machine."""
        return self.all_processes[0].host

    @property
    def master_id(self) -> int:
        return self.all_processes[0].id

    def run(self) -> List[threading.Thread]:
        threads = []
        # run FD
        threads.extend(self.fd.run())

        # run introducer
        threads.append(threading.Thread(target=self.introducer))

        # run sdfs
        # threads.append(threading.Thread(target=self.commander))
        threads.append(threading.Thread(target=self.tcp_receiver_from_master))
        threads.append(threading.Thread(target=self.tcp_receiver_from_client))
        threads.append(threading.Thread(target=self.failure_received))

        return threads

    # can DNS assign id instead of introducer
    def introducer(self, s=...):
        if s is Ellipsis:
            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

        s.bind(("", PORT_FDINTRODUCER))
        while True:
            data, addr = s.recvfrom(32 * 1024)

            if socket_should_stop(data):
                break

            if data:
                message: Message = pickle.loads(data)

                if message.message_type == "JOIN":
                    info = {
                        "ML": self.fd.ml,
                        "FT": self.ft,
                    }
                    new_msg = self.__generate_message("UPDATE", info)
                    s.sendto(pickle.dumps(new_msg), addr)  # TODO: this ``addr`` might be wrong
        s.close()

    def ask_dns(self, message: Message) -> Message:
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
            # Send udp ``message`` to dns server
            s.sendto(message, (DNS_SERVER_HOST, DNS_SERVER_PORT))
            data = self.recv_message_udp(s)
            resp: Message = pickle.loads(data)
            return resp

    def failure_received(self):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            s.bind(("", PORT_SDFS_GETFAILURE))
            s.listen()
            while True:
                conn, addr = s.accept()
                data = conn.recv(4096)
                message: Message = pickle.loads(data)
                leaved = message.content["leaved_id"]
                time_stamp = message.content["time_stamp"]
                print(f"-----SDFS: received id{leaved} leaved at {time_stamp}")
                self.ft_lock.acquire()
                self.delete_replicas(leaved)
                self.ft_lock.release()

    def delete_replicas(self, leaved_id: int):
        print("Delete replicas")
        for filename, versionInfo in self.ft.files.items():
            need_send_update = False
            info = versionInfo[0]
            if leaved_id not in info.replicas:
                continue

            for i in versionInfo:
                i.replicas.discard(leaved_id)
            lowest_id = min(info.replicas)
            new_replica = self.id

            # the machine with the highest id do the recover and send out ft update
            if self.id == lowest_id:
                print("I am the lowest id")
                self.fd.ml_lock.acquire()
                if len(self.fd.ml) > 3:
                    alive_id = set(self.all_processes_ids) - info.replicas
                    new_replica = random.choice(list(alive_id))
                    need_send_update = True
                self.fd.ml_lock.release()

            if need_send_update:
                print(f"I need to send updata and do the recover and new replic is {new_replica}")
                for i in versionInfo:
                    i.replicas.add(new_replica)

                # send update message to all other machines
                content = {
                    "fname": filename,
                    "versionInfo": versionInfo,
                    "leaved_id": leaved_id,
                    "new_replica": new_replica,
                }
                message = self.__generate_message("FT FAILURE UPDATE", content=content)
                self.multicast(
                    message,
                    set(self.all_processes_ids) - set([self.id]) - set([leaved_id]),
                    PORT_TCP_MASTERSERVER,
                )

                # prefix = "peiranw3" + "@" + self.__id_to_host(new_replica) + ":"
                # for v in versionInfo:
                #     p = subprocess.Popen(
                #         [
                #             "scp",
                #             "-o",
                #             "StrictHostKeyChecking=no",
                #             "-o",
                #             "UserKnownHostsFile=/dev/null",
                #             "-q",
                #             self.__remote_to_sdfspath(v.fname),
                #             prefix + self.__remote_to_sdfspath(v.fname),
                #         ]
                #     )

    def tcp_receiver_from_master(self):  # client
        """Receives TCP message from master."""
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            s.bind(("", PORT_TCP_MASTERSERVER))
            s.listen()
            while True:
                conn, addr = s.accept()
                with conn:
                    data = self.recv_message(conn)

                    message: Message = pickle.loads(data)
                    if message.message_type == "REQ WRITE":
                        # write to local file, update ft
                        fname = message.content["remote"]
                        version = message.content["version"]
                        offset = (message.content["chunk"] - 1) * FILE_CHUNK_SIZE
                        payload = message.content["payload"]
                        self.__write_to_local(
                            self.__remote_to_sdfspath(fname, version), payload, offset, "ab"
                        )

                        targets = message.content["targets"]
                        poped: SDFSFile = self.ft.insert(fname, targets, version)
                        self.__remove_poped_sdfsfile(poped)
                        # print(f"... Replica wrote to sdfs fname = {fname}")

                        # reply with confirm
                        confirm_message = self.__generate_message("WRITE CONFIRM")
                        conn.sendall(pickle.dumps(confirm_message))

                    elif message.message_type == "FT UPDATE":
                        # update ft
                        fname = message.content["remote"]
                        targets = message.content["targets"]
                        version = message.content["version"]
                        self.ft.insert(fname, targets, version)
                        # no need to remove poped SDFSFile

                    elif message.message_type == "REQ DELETE":
                        # delete from local sdfs, update ft
                        fname = message.content["remote"]
                        for file in self.ft.files[fname]:
                            os.remove(self.__remote_to_sdfspath(fname, file.version))
                        self.ft.delete(fname)

                        # reply with confirm
                        confirm_message = self.__generate_message("DELETE CONFIRM")
                        conn.sendall(pickle.dumps(confirm_message))

                    elif message.message_type == "FT DELETE":
                        # delete from ft
                        fname = message.content["remote"]
                        self.ft.delete(fname)

                    elif message.message_type == "REQ READ":
                        # request for reading a specified chunk of a file
                        remote_filename = message.content["remote"]
                        version = message.content["version"]
                        fileInfo = self.ft.get(remote_filename, version)
                        if fileInfo is None:
                            continue
                        path = self.__remote_to_sdfspath(fileInfo.fname)
                        chunk = message.content["chunk"]

                        offset = (chunk - 1) * FILE_CHUNK_SIZE
                        payload = self.__read_from_local(path, offset)

                        content = {"payload": payload, "chunk": chunk}
                        resp_message = self.__generate_message("RESP READ", content=content)
                        conn.sendall(pickle.dumps(resp_message))

                    elif message.message_type == "REQ VERSION":
                        remote = message.content["remote"]
                        version = self.__get_latest_verion_number(remote)
                        fsize = self.__local_file_size(self.__remote_to_sdfspath(remote, version))
                        resp_message = self.__generate_message("RESP VERSION", content={"version": version, "fsize": fsize})
                        conn.sendall(pickle.dumps(resp_message))

                    elif message.message_type == "FT FAILURE UPDATE":
                        # in this case, we need to update the ft since there is a crushed machine
                        print("I received Failure Update!!")
                        self.ft_lock.acquire()
                        new = message.content["new_replica"]
                        print(message.content["versionInfo"])
                        if self.id == new:
                            self.ft.update(
                                message.content["fname"],
                                message.content["versionInfo"],
                                message.content["leaved_id"],
                                self.id,
                            )
                        else:
                            self.ft.update(
                                message.content["fname"],
                                message.content["versionInfo"],
                                message.content["leaved_id"],
                                -1,
                            )
                        self.ft_lock.release()

    def tcp_receiver_from_client(self):  # master
        """Receives TCP message from client.

        All direct write/read to/from sdfs directory comes from this method.
        """
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            s.bind(("", PORT_TCP_CLIENTMASTER))
            s.listen()

            while True:
                conn, addr = s.accept()
                with conn:
                    data = self.recv_message(conn)

                    message: Message = pickle.loads(data)
                    if message.message_type == "CLIENT WRITE":
                        # write to replicas and wait for confirmation
                        # print("... Server received REQ WRITE")
                        payload = message.content["payload"]
                        file = self.ft.get(message.content["remote"])
                        targets: set = (
                            file.replicas if file is not None else self.__default_replicas(message.content["remote"], message.id)
                        )
                        remote = message.content["remote"]
                        chunk = message.content["chunk"]
                        total_chunks = message.content["total_chunks"]
                        version = self.__get_latest_verion_number(remote) + int(chunk == 1)
                        content = {
                            "payload": payload,
                            "targets": targets,
                            "remote": remote,
                            "version": version,
                            "chunk": chunk,
                        }
                        write_message: Message = self.__generate_message("REQ WRITE", content)

                        # WRITE CONFIRM received from replica processes
                        confirmation = self.multicast(write_message, targets, PORT_TCP_MASTERSERVER, -1, True)
                        write_success = confirmation >= self.W
                        if write_success:
                            resp_message: Message = self.__generate_message("WRITE CONFIRM")
                            conn.sendall(pickle.dumps(resp_message))  # reply with confirmation
                            # print(f"... Sent WRITE CONFIRM with W = {self.W}")
                            conn.shutdown(socket.SHUT_RDWR)
                        else:
                            resp_message: Message = self.__generate_message("WRITE FAILED")
                            conn.sendall(pickle.dumps(resp_message))  # reply with failure
                            conn.shutdown(socket.SHUT_RDWR)

                        # Mulicast to other processes
                        if chunk == total_chunks and write_success:  # last chunk confirmed
                            ids = set(self.all_processes_ids) - targets
                            print(ids)
                            print(targets)
                            multicast_message = self.__generate_message("FT UPDATE", write_message.content)
                            self.multicast(multicast_message, ids, PORT_TCP_MASTERSERVER)

                    elif message.message_type == "REQ DELETE":
                        remote = message.content["remote"]
                        targets = set()
                        for file_version in self.ft.files[remote]:
                            targets.update(file_version.replicas)
                        print("... Server received REQ DELETE")
                        confirmation = self.multicast(message, targets, PORT_TCP_MASTERSERVER, -1, True)

                        # reply with DELETE CONFIRM if all replicas have been deleted
                        if confirmation == len(targets):
                            resp_message: Message = self.__generate_message("DELETE CONFIRM")
                            conn.sendall(pickle.dumps(resp_message))  # reply with delete confirmation
                            print(f"... Sent DELETE CONFIRM to client")
                            conn.shutdown(socket.SHUT_RDWR)
                        else:
                            resp_message: Message = self.__generate_message("DELETE FAILED")
                            conn.sendall(pickle.dumps(resp_message))  # reply with failure
                            conn.shutdown(socket.SHUT_RDWR)

                        # Multicast to other processes
                        ids = set(self.all_processes_ids) - targets
                        multicast_message = self.__generate_message("FT DELETE", content=message.content)
                        self.multicast(multicast_message, ids, PORT_TCP_MASTERSERVER)

    def write_to(self, message: Message, remote_host: str, remote_port: int, confirm: bool = True) -> int:
        """Writes ``message`` to address (remote_host, remote_port).
        ``message`` should have ``m_type = REQ WRITE`` and include filename and payload in ``content``.

        Waits for confirmation, return ``1`` if resp received."""
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:  # tcp
            # print(f"... ... Write {message.message_type} to {remote_host}")
            try:
                s.connect((remote_host, remote_port))
                s.sendall(pickle.dumps(message))
                s.shutdown(socket.SHUT_WR)
            except socket.error:
                return 0
            if confirm:
                # wait for confirm
                data = s.recv(1024)
                confirmation: Message = pickle.loads(data)
                confirmed = confirmation.message_type.endswith("CONFIRM")
                if confirmed:
                    self.put_ack_lock.acquire()
                    self.put_ack.setdefault(message.mid, 0)
                    self.put_ack[message.mid] += 1
                    self.put_ack_lock.release()
                return confirmed
            return 1

    def recv_chunk(self, sdfsfilename: str, chunk: int, remote_host: str, version: int = -1) -> bytes:
        """Reads a chunk from ``sdfsfilename`` from a host ``remote_host``."""
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:  # tcp
            message = self.__generate_message("REQ READ", content={"remote": sdfsfilename, "version": version, "chunk": chunk})
            try:
                s.connect((remote_host, PORT_TCP_MASTERSERVER))
                s.sendall(pickle.dumps(message))
                s.shutdown(socket.SHUT_WR)
            except socket.error:
                return b""

            try:
                # Recv chunk
                s.settimeout(1.2)
                data = self.__recv_message(s)
                chunk_message: Message = pickle.loads(data)
                return chunk_message.content["payload"]
            except socket.timeout:
                return b""

    def read_replica_version(self, sdfsfilename: str, remote_host: str):
        """Reads the highest version number and file size of ``sdfsfilename`` from ``remote_host``."""
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            # Ask a replica for its latest version number of a file
            message = self.__generate_message("REQ VERSION", {"remote": sdfsfilename})
            try:
                s.connect((remote_host, PORT_TCP_MASTERSERVER))
                s.sendall(pickle.dumps(message))
                s.shutdown(socket.SHUT_WR)
            except socket.error:
                return -1, -1

            try:
                s.settimeout(1.0)
                packet = s.recv(1024)  # recv version number
                version_message: Message = pickle.loads(packet)
                version = version_message.content["version"]
                fsize = version_message.content["fsize"]
                return version, fsize
            except socket.timeout:
                return -1, -1

    def multicast(
        self, message: Message, targets: Set[int], port: int, confirm: int = 0, threads: bool = False
    ) -> int:
        """Multicasts ``message`` to all target in ``targets`` via TCP."""
        # print(f"----I am multicast: {message.message_type} and target is: {targets}")
        if confirm > 0:
            self.put_ack[message.mid] = 0

        if threads is False:
            # Simply send tcp to targets
            for target in targets:
                self.write_to(message, self.__id_to_host(target), port, confirm=confirm)
        else:
            for target in targets:
                host = self.__id_to_host(target)
                threading.Thread(target=self.write_to, args=(message, host, port, confirm, )).start()
        
        confirmation = 0
        if confirm == -1:
            confirm = self.W
        if confirm > 0:
            # Monitor confirmation message
            confirmation = self.__monitor_confirmation(message.mid, confirm)

            del self.put_ack[message.mid]
        return confirmation


    def put(self, localfilename: str, sdfsfilename: str) -> int:
        """Puts localfilename to sdfsfilename.

        A ``Message`` with ``m_type = REQ WRITE`` is sent to all replicas, along with file content.
        """
        if not os.path.isfile(localfilename):
            print(f"... No such local file: {localfilename}")
            return 0

        fsize = self.__local_file_size(localfilename)
        offset, chunk = 0, 1
        total_chunks = fsize // FILE_CHUNK_SIZE + int((fsize % FILE_CHUNK_SIZE) > 0)

        while offset < fsize:
            payload = self.__read_from_local(localfilename, offset)

            # Transfer file to master via TCP
            content = {"remote": sdfsfilename, "payload": payload, "chunk": chunk, "total_chunks": total_chunks}
            message = self.__generate_message("CLIENT WRITE", content=content)

            # send to master
            # master will arrange writes, update replicas, collect confirm, and multicast
            # print("... Sent CLIENT WRITE to server")
            confirmed = self.multicast(message, set([self.master_id]), PORT_TCP_CLIENTMASTER, confirm=1)
            
            if confirmed:
                offset += FILE_CHUNK_SIZE
                chunk += 1

        return confirmed

    def get(self, sdfsfilename: str, localfilename: str, num_versions: int = -1) -> int:
        """Gets sdfsfilename to localfilename."""
        # check wether exsits
        if not sdfsfilename in self.ft:
            print(f"... No such sdfs file: {sdfsfilename}")
            return 0

        fileInfo: SDFSFile = self.ft.files[sdfsfilename][-1]  # -1 is latest
        replicas = fileInfo.replicas  # all versions should have the same replica set

        max_version, max_version_replica, max_version_fsize = self.__get_max_version_info(sdfsfilename, replicas)

        if max_version == -1:
            return 0  # read failed

        if num_versions == -1:
            versions = [-1]
        else:
            versions = list(range(max_version, max(max_version - int(num_versions), 0), -1))

        temp_localfilename = localfilename + ".tmp"  # overwrite local file

        offset = 0
        read_target = max_version_replica  # from whom to read
        for i in range(len(versions)):
            version = versions[i]
            if len(versions) > 1:  # get-versions instead of get
                self.__write_to_local(
                    temp_localfilename, f"\n===== Version {version} =====\n".encode(), offset, "ab"
                )  # write delimeter

            chunk = 1
            total_chunks = max_version_fsize // FILE_CHUNK_SIZE + int((max_version_fsize % FILE_CHUNK_SIZE) > 0)
            new_max_offset = offset + max_version_fsize

            retry = 0
            while offset < new_max_offset:
                payload = self.recv_chunk(sdfsfilename, chunk, self.__id_to_host(read_target), version)
                retry += 1
                if len(payload) > 0:
                    offset += FILE_CHUNK_SIZE
                    chunk += 1
                    retry = 0
                    self.__write_to_local(temp_localfilename, payload, offset, "ab")
                if retry == GET_RETRY:
                    # Has already tried several times
                    break

        # Check temp_localfilename, 
        # if valid, remove localfilename if exists, rename temp_localfilename to localfilename
        if self.__local_file_size(temp_localfilename) == max_version_fsize:
            self.__remove_from_local(localfilename)
            os.rename(temp_localfilename, localfilename)
            return 1
        else:
            self.__remove_from_local(temp_localfilename)
            
        return 0

    def delete(self, sdfsfilename: str) -> bool:
        """Deletes sdfsfilename from SDFS."""
        if sdfsfilename not in self.ft.files:
            print(f"... No such sdfs file: {sdfsfilename}")
            return False

        message: Message = self.__generate_message("REQ DELETE", content={"remote": sdfsfilename})
        confirmed = self.write_to(message, self.master, PORT_TCP_CLIENTMASTER, confirm=True)
        return confirmed

    def join(self):
        assigned_id, ft = self.fd.join()
        if assigned_id != -1:
            self.id = assigned_id
        if ft is not None:
            self.ft = ft
        return self.id

    def commander(self):
        """Receives and responds to user command.

        Commands:
        - put localfilename sdfsfilename
        - get sdfsfilename localfilename
        - delete sdfsfilename
        - ls sdfsfilename: list where sdfsfilename is stored
        - store: list all files in the current machine
        - get-versions sdfsfilename num-versions localfilename
        """

        # Starts the command line interface
        print()
        print("SDFS version 0.0.1")

        while True:
            command = input(">>> ")
            argv = command.split()
            if len(argv) == 0:
                continue
            if argv[0] == "put" and len(argv) == 3:
                # put localfilename sdfsfilename
                if self.put(argv[1], argv[2]):
                    print(f"{command}: success")
                else:
                    print(f"{command}: failed")

            elif argv[0] == "get" and len(argv) == 3:
                # get sdfsfilename localfilename
                self.get(argv[1], argv[2])

            elif argv[0] == "delete" and len(argv) == 2:
                # delete sdfsfilename
                if self.delete(argv[1]):
                    print(f"{command}: success")
                else:
                    print(f"{command}: failed")

            elif argv[0] == "ls" and len(argv) == 2:
                # ls sdfsfilename
                self.ft.print("ls", fname=argv[1])

            elif argv[0] == "store" and len(argv) == 1:
                # store
                self.ft.print("store", id=self.id)

            elif argv[0] == "get-versions" and len(argv) == 4:
                # get-versions sdfsfilename num-versions localfilename
                self.get(argv[1], argv[3], argv[2])

            ### FD commands ###
            elif command.startswith("join"):
                self.__clear_sdfs_directory()  # clear stall content
                self.join()

            elif command.startswith("leave"):
                self.fd.leave()
                self.ft.clear()
                self.__clear_sdfs_directory()
                # and clear ft and local files

            elif command.startswith("ml"):
                print(self.fd.ml)

            elif command.startswith("id"):  # list itself
                print(self.id)

            else:
                print(f"[ERROR] Invalid command: {command}")

    # Utility functions for SDFS
    def __generate_message(self, m_type: str, content: Any = None) -> Message:
        """Generates message for all communications."""
        return Message(self.id, self.host, self.port, time.time(), m_type, content)

    __default_version = 0

    def __default_replicas(self, filename: str, id=...) -> set:
        """Default replicas are the DEFAULT_NUM_NEIGHBORS neighbors in ML, including ``self.id``."""
        if id is Ellipsis:
            id = self.id
        self.fd.ml_lock.acquire()
        l = len(self.fd.ml)
        neighbors = []
        if l <= 4:
            neighbors = self.fd.ml.neighbors(id, DEFAULT_NUM_REPLICAS, include_self=True)
        else:
            for i in range(DEFAULT_NUM_REPLICAS):
                idx = (hash(filename) + i) % l
                neighbors.append(self.fd.ml.content[idx])
        self.fd.ml_lock.release()
        print(f"default replica: {neighbors}")
        return set([neighbor.id for neighbor in neighbors])

    def __id_to_host(self, id: int) -> str:
        """Finds host name with id."""
        return self.fd.ml[id].host

    def __remote_to_sdfspath(self, remote: str, version: int = -1) -> str:
        """Defines sdfsfilename -> sdfsfilepath. If ``version`` not given, assume ``remote`` also includes version info."""
        if version == -1:
            return os.path.join(self.dir, remote)
        else:
            return os.path.join(self.dir, self.ft._remote_to_sdfsfname(remote, version))

    @staticmethod
    def recv_message(conn: socket.socket) -> bytes:
        """Receives data with tcp."""
        data = bytearray()
        while True:
            packet = conn.recv(32 * 1024)
            if not packet:
                break
            data.extend(packet)
        return data

    @staticmethod
    def recv_message_udp(s: socket.socket) -> bytes:
        data = bytearray()
        while True:
            s.settimeout(2)
            packet = s.recvfrom(4 * 1024)
            if not packet:
                break
            data.extend(packet)
        return data
   
    @staticmethod
    def __read_from_local(fpath: str, off: int = 0) -> bytes:
        """Defines how to read from ``fpath``."""
        with open(fpath, "rb") as f:
            f.seek(off)
            return f.read(FILE_CHUNK_SIZE)

    @staticmethod
    def __write_to_local(fpath: str, payload: bytes, off: int = 0, mode="wb"):
        """Defines how to write ``payload`` to local ``fpath``."""
        with open(fpath, mode) as f:
            f.seek(off)
            f.write(payload)

    @staticmethod
    def __remove_from_local(fpath: str):
        if os.path.exists(fpath):
            os.remove(fpath)
    
    @staticmethod
    def __local_file_size(fpath: str) -> int:
        if os.path.exists(fpath):
            return os.path.getsize(fpath)
        return 0
    
    def __remove_poped_sdfsfile(self, file: SDFSFile):
        """Removes a SDFSFile from local sdfs directory."""
        if file is None:
            return
        os.remove(self.__remote_to_sdfspath(file.fname))
    
    def __clear_sdfs_directory(self):
        """Removes all files under sdfs/ directory."""
        for fname in os.listdir(self.dir):
            fpath = os.path.join(self.dir, fname)
            if os.path.isfile(fpath) or os.path.islink(fpath):
                os.unlink(fpath)

    def __get_latest_verion_number(self, remote: str) -> int:
        # !!! ONLY master could use this
        # master assigns a version number.
        # This could be safer than having each process deciding version number.
        # This guarantees all version 10, for example, are the same in each replica.
        version = self.ft.get(remote)
        version = self.__default_version if version is None else version.version
        return version

    def __get_max_version_info(self, sdfsfilename: str, replicas: set) -> Tuple[int, int, int]:
        """Finds the replica with the highest version of a ``sdfsfilename``.
        
        Returns:
            version, replica, fsize
        """
        max_version, max_version_replica, max_version_fsize = 0, 0, -1
        for r in replicas:
            version, fsize = self.read_replica_version(sdfsfilename, self.__id_to_host(r))
            if version > max_version:
                max_version, max_version_replica, max_version_fsize = version, r, fsize

        return max_version, max_version_replica, max_version_fsize

    def __monitor_confirmation(self, mid, confirm: int) -> int:
        now = time.time()
        while time.time() - now < PUT_TIMEOUT:
            time.sleep(0.1)
            self.put_ack_lock.acquire()
            confirmation = self.put_ack[mid]
            self.put_ack_lock.release()
            if confirmation >= confirm:
                return confirmation
            time.sleep(0.1)

        return 0


if __name__ == "__main__":
    sdfs = SDFS()
    sdfs.run()
