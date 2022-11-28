import socket
import unittest

from ..utils import TestSocket


class TestTestSocket(unittest.TestCase):
    def setUp(self) -> None:
        self.s = TestSocket("192.168.0.1", 1234)

    def test_bind(self):
        self.s.bind(("", 1234))
        self.assertIn(self.s.host, self.s.fd)
        self.assertIn(1234, self.s.fd[self.s.host])

    def test_sendto(self):
        # Test: valid address
        with self.assertRaises(ValueError):
            self.s.sendto(b"abc", (1, 2, 3, 4))

        # Test: sendto should not be queued if no one is
        self.s.sendto(b"abc", ("host 1", 123))
        self.assertNotIn("host 1", self.s.fd)

        # Test: sendto self and recvfrom self
        self.s.bind(("host 1", 123))  # start listening from ("host 1", 123)
        self.assertIn("host 1", self.s.fd)
        self.assertIn(123, self.s.fd["host 1"])
        self.s.sendto(b"abc", ("host 1", 123))
        self.assertTrue(self.s.fd["host 1"][123].not_empty)

        data, addr = self.s.recvfrom(4096)
        self.assertEqual(data, b"abc")
        self.assertEqual(addr, self.s.addr)

    def test_two_sockets(self):
        """Test communication between two TestSocket."""
        s2 = TestSocket("192.168.0.2", 2345)

        s2.bind(("", 2345))
        self.s.sendto(b"abc", ("192.168.0.2", 2345))
        self.assertTrue(self.s.fd[s2.host][2345].not_empty)

    def test_settimeout(self):
        """Test if s.settimeout works."""
        self.s.settimeout(0.5)

        self.s.bind(("192", 123))
        with self.assertRaises(socket.timeout):
            self.s.recvfrom(1024)

    def test_close(self):
        self.s.bind(("192", 123))
        self.assertIn("192", self.s.fd)

        self.s.close()
        self.assertIn("192", self.s.fd, "bind_host should still in fd")
        self.assertNotIn(123, self.s.fd["192"])

    def test_closed(self):
        """Test s.closed property."""
        self.assertTrue(self.s.closed, "Fresh socket is closed")

        self.s.bind(("12", 123))
        self.assertFalse(self.s.closed, "socket that's binded is not closed")

        self.s.close()
        self.assertTrue(self.s.closed, "socket that's called s.closed() is closed")

        self.s = TestSocket("192.168.0.1", 1234)
        self.s.bind(("", 1234))
        self.s.sendto(b"abc", ("123", 123))
        self.s.close()
        self.assertTrue(self.s.closed)
