"""Integration tests on FailureDetector."""
import pickle
import time
import threading
import unittest
from typing import List

from src.DNSserver import DNSserver
from src.fd import _MLEntry
from src.sdfs import SDFS
from src.utils import getLogger, Message
from src.config import *
from ..utils import TestSocket, catch_threading_exception, TestThread


class TestFDIntegration(unittest.TestCase):

    @classmethod
    def setUpClass(cls) -> None:
        cls.e1 = _MLEntry(1, "192", 123, 0.2)
        cls.e2 = _MLEntry(2, "123", 234, 0.3)
        cls.e3 = _MLEntry(3, "135", 100, 0.8)
        cls.e4 = _MLEntry(4, "987", 300, 1.25)
        cls.e5 = _MLEntry(5, "394", 400, 1.75)
        cls.logger = getLogger(__name__, "testfd_integration.log")

        cls.host = "daemon"
        cls.port = 1234

    def setUp(self) -> None:
        self.dns = DNSserver()

    def setUpMachines(self, n: int = 1) -> None:
        self.machines: List[SDFS] = []
        self.introducer_sockets: List[TestSocket] = []
        self.join_sockets: List[TestSocket] = []
        self.receiver_sockets: List[TestSocket] = []

        for i in range(n):
            # Setup SDFS
            sdfs = SDFS(f"host {i}")
            sdfs.fd.logger = self.logger
            self.machines.append(sdfs)

            # Setup sockets for each SDFS
            self.introducer_sockets.append(TestSocket(sdfs.host, PORT_FDINTRODUCER))
            self.join_sockets.append(TestSocket(sdfs.fd.host, PORT_FDUPDATE))
            self.receiver_sockets.append(TestSocket(sdfs.fd.host, sdfs.fd.port))

        # Setup daemon and dns socket
        self.s = TestSocket(self.host, self.port)  # collecting test results
        self.dns_socket = TestSocket(DNS_SERVER_HOST, DNS_SERVER_PORT)

    def join_machine(self, i: int) -> bool:
        with catch_threading_exception() as cm:
            join_thread = TestThread(target=self.machines[i].fd.join, args=(self.join_sockets[i], ))
            join_thread.start()
            ret = join_thread.join(10)
            if cm.exc_value is not None:
                raise cm.exc_value from None
            if ret is None: return False
            assigned_id, ft = ret
            self.assertTrue(self.join_sockets[i].closed)
            if assigned_id > -1:
                self.machines[i].id = assigned_id
            if ft is not None:
                self.machines[i].ft = ft
            return assigned_id > -1 and ft is not None

    def start_machine(self, i: int):
        if self.machines[i]._started is False:
            introducer_thread = TestThread(target=self.machines[i].introducer, args=(self.introducer_sockets[i], ))
            receiver_thread = TestThread(target=self.machines[i].fd.receiver, args=(self.receiver_sockets[i], ))
            introducer_thread.start()
            receiver_thread.start()
            self.machines[i]._started = True

    def stop_machine(self, i: int):
        self.send_stop(self.introducer_sockets[i])  # stops introducer
        self.send_stop(self.receiver_sockets[i])  # stops receiver
        self.machines[i]._started = False

    def send_stop(self, s: TestSocket):
        self.s.sendto(b"stop", s.addr)

    def assert_sockets_closed(self, n: int = ...) -> None:
        for i in range(n):
            self.assertTrue(self.join_sockets[i].closed, f"join socket {i} is not closed")
            self.assertTrue(self.introducer_sockets[i].closed, f"introducer socket {i} is not closed")
            self.assertTrue(self.receiver_sockets[i].closed, f"receiver socket {i} is not closed")
        self.assertTrue(self.dns_socket.closed, "DNS socket is not closed")
        self.assertTrue(self.s.closed)

    def assert_distinct_id(self):
        """Test if self.machines all have distinct ids."""
        ids = []
        for sdfs in self.machines:
            self.assertEquals(sdfs.id, sdfs.fd.id)
            self.assertNotIn(sdfs.id, ids)
            ids.append(sdfs.id)

    def test_join_one_machine(self):
        """Test fd.join() method with only one machine."""
        n = 1
        
        # Test: join running by itself would not cause an error
        self.setUpMachines(n)
        with catch_threading_exception() as cm:
            self.assertFalse(self.join_machine(0))  # assert cannot join

            # Check if a GET_INTRODUCER message is sent to DNS
            self.assertEqual(len(self.s.packets), 1)
            self.assertFalse(self.s.packets[0].sent, "message should not be sent")
            self.assertEqual(pickle.loads(self.s.packets[0].data).message_type, "GET_INTRODUCER")

            self.assert_sockets_closed(n)

            if cm.exc_value is not None:
                self.fail("fd.join() running by itself without introducer/dns should not raise an error")

        # Test: communication between fd.join and DNSserver
        self.setUpMachines(n)
        with catch_threading_exception() as cm:
            dns_thread = TestThread(target=self.dns.server, args=(self.dns_socket, ))
            dns_thread.start()

            self.assertFalse(self.join_machine(0))  # assert cannot join

            if cm.exc_value is not None:
                raise cm.exc_value from None
            
            self.send_stop(self.dns_socket)  # stops DNS

            # Check messages: GET_INTRODUCER, RESP_INTRODUCER, JOIN
            # s.packets: GET_INTRODUCER (from prev test), GET_INTRODUCER, RESP_INTRODUCER, JOIN, stop
            self.assertEqual(len(self.s.packets), 5)  # s.packets is not reset by self.setUpSockets

            self.assertTrue(self.s.packets[1].sent)  # GET_INTRODUCER
            self.assertEqual(pickle.loads(self.s.packets[1].data).message_type, "GET_INTRODUCER")

            self.assertTrue(self.s.packets[2].sent)  # RESP INTRODUCER
            resp_msg: Message = pickle.loads(self.s.packets[2].data)
            self.assertEqual(resp_msg.message_type, "RESP_INTRODUCER")
            self.assertIn("introducer_host", resp_msg.content)  # these three fields must exist in RESP_INTRODUCER
            self.assertIn("introducer_port", resp_msg.content)
            self.assertIn("assigned_id", resp_msg.content)

            self.assertFalse(self.s.packets[3].sent)  # JOIN
            self.assertEqual(pickle.loads(self.s.packets[3].data).message_type, "JOIN")

            time.sleep(0.5)  # necessary
            self.assert_sockets_closed(n)

        # Test: fd.join, DNSserver, introducer all up
        self.setUpMachines(n)
        with catch_threading_exception() as cm:
            dns_thread = TestThread(target=self.dns.server, args=(self.dns_socket, ))
            introducer_thread = TestThread(target=self.machines[0].introducer, args=(self.introducer_sockets[0], ))

            dns_thread.start()
            introducer_thread.start()

            self.assertTrue(self.join_machine(0))  # assert join success

            if cm.exc_value is not None:
                raise cm.exc_value from None

            self.send_stop(self.dns_socket)  # stops DNS
            self.send_stop(self.introducer_sockets[0])  # stops introducer

            # Messages: GET_INTRODUCER, RESP_INTRODUCER, JOIN, UPDATE, (no JOIN to neighbor), stop, stop
            # self.assertEqual(len(self.s.packets), 11)
            self.assertEqual(pickle.loads(self.s.packets[7].data).message_type, "JOIN")
            self.assertTrue(self.s.packets[7].sent)  # JOIN -> introducer
            
            self.assertEqual(pickle.loads(self.s.packets[8].data).message_type, "UPDATE", "introducer probably does not respond to JOIN")
            self.assertTrue(self.s.packets[8].sent)  # UPDATE -> join host

            time.sleep(0.5)  # necessary
            self.assert_sockets_closed(n)

    def test_join_more_machines(self):
        n = 10

        # Test: join running by itself would not cause an error
        self.setUpMachines(n)
        with catch_threading_exception() as cm:
            dns_thread = threading.Thread(target=self.dns.server, args=(self.dns_socket, ))
            dns_thread.start()

            for i in range(n):
                self.start_machine(i)
                self.assertTrue(self.join_machine(i))
                time.sleep(0.1)  # without this, JOIN could fail with missing ML entries

            if cm.exc_value is not None:
                raise cm.exc_value from None

            # Check ML
            for machine in self.machines:
                ml = machine.fd.ml
                self.assertEqual(len(ml), n, f"machine {machine.id} misses: {set(range(n)) - set([_ml.id for _ml in ml])}")
                for i in range(n):
                    self.assertIn(i, ml)

            self.send_stop(self.dns_socket)
            for i in range(n):
                self.stop_machine(i)
            time.sleep(0.5)

            self.assert_distinct_id()
            self.assert_sockets_closed(n)
