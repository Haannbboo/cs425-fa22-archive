import pickle
import unittest
from typing import List

from src.sdfs.fd import MembershipList, _MLEntry, _MsgCache, FailureDetector
from src.config import DEFAULT_NUM_NEIGHBORS
from src.sdfs.utils import Message, getLogger, get_host
from ..utils import TestSocket


class TestMLEntry(unittest.TestCase):
    """Test _MLEntry's methods."""
    @classmethod
    def setUpClass(cls) -> None:
        cls.e1 = _MLEntry(1, "192", 123, 0.2)
        cls.e2 = _MLEntry(1, "192", 123, 0.2)
        cls.e3 = _MLEntry(1, "193", 123, 0.2)
        cls.e4 = _MLEntry(1, "192", 1234, 0.2)
        cls.e5 = _MLEntry(1, "192", 123, 0.3)
        cls.e6 = _MLEntry(2, "192", 123, 0.2)

    
    def test_eq(self):
        self.assertEqual(self.e1, self.e2, "Two _MLEntry are equal if their id, host, port, timestamp are all equal.")
        self.assertNotEqual(self.e1, self.e3, "Two _MLEntry with different host are not equal.")
        self.assertNotEqual(self.e1, self.e4, "Two _MLEntry with different port number are not equal.")
        self.assertNotEqual(self.e1, self.e5, "Two _MLentry with different timestamp are not equal.")
        self.assertNotEqual(self.e1, self.e6)
    
    def test_lt(self):
        self.assertTrue(self.e1 < self.e6, f"e1.id = {self.e1.id} < e6.id = {self.e6.id}")
        self.assertFalse(self.e1 < self.e1, f"e1.id = {self.e1.id} == e1.id = {self.e1.id}")
        self.assertFalse(self.e6 < self.e1)

    def test_getitem(self):
        self.assertEqual(self.e1["id"], 1)
        self.assertEqual(self.e2["host"], "192")
        with self.assertRaises(AttributeError):
            self.e1["something weird"]
    
    def test_setitem(self):
        entry = _MLEntry(1, "192", 123, 0.2)
        self.assertEqual(entry.id, 1)
        entry["id"] = 10
        self.assertEqual(entry.id, 10)
        entry["timestamp"] += 1
        self.assertEqual(entry.timestamp, 1.2)
        # __setitem__ on none-existing fields should be fine
    
    def test_get(self):
        self.assertEqual(self.e1.get("id"), 1)
        self.assertIsNone(self.e1.get("something weird"))
        self.assertEqual(self.e1.get("something", 1), 1)

    def test_ml_entry(self):
        # From previous mp2 commits
        entry = _MLEntry(1, "local", 12345, 1.25)
        self.assertIsInstance(entry, _MLEntry)

        self.assertEqual(entry["id"], 1)
        self.assertEqual(entry["host"], "local")
        self.assertIsNone(entry.get("something not here"))

        entry["id"] = 2
        self.assertEqual(entry["id"], 2)
        self.assertEqual(entry["port"], 12345)
        entry["timestamp"] += 2
        self.assertEqual(entry["timestamp"], 3.25)

        self.assertEqual(entry.id, 2)
        self.assertEqual(entry.port, 12345)


class TestML(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.e1 = _MLEntry(1, "192", 123, 0.2)
        cls.e2 = _MLEntry(1, "192", 123, 0.2)
        cls.e3 = _MLEntry(1, "193", 123, 0.2)
        cls.e4 = _MLEntry(1, "192", 1234, 0.2)
        cls.e5 = _MLEntry(1, "192", 123, 0.3)
        cls.e6 = _MLEntry(2, "192", 123, 0.2)

    def setUp(self) -> None:
        self.ml = MembershipList()

    def test_append(self):
        # This test comes first in TestML.
        self.ml.append(self.e1)
        self.assertEqual(len(self.ml._content), 1)
        self.assertIn(self.e1, self.ml._content)
        self.ml.append(self.e4)
        self.assertIn(self.e4, self.ml._content)
        self.assertNotIn(self.e6, self.ml._content)
        with self.assertRaises(ValueError):
            self.ml.append(123)

    def test_len(self):
        self.ml.append(self.e1)
        self.assertEqual(len(self.ml), 1)
        self.ml.append(self.e3)
        self.assertEqual(len(self.ml), 2)

    def test_contains(self):
        self.ml.append(self.e1)
        self.assertIn(self.e1.id, self.ml)
        self.assertNotIn(100, self.ml)
        self.ml.append(self.e6)
        self.assertIn(self.e6.id, self.ml)

    def test_getitem(self):
        self.ml.append(self.e1)
        self.assertEqual(self.e1, self.ml[self.e1.id])
        with self.assertRaises(KeyError):
            self.ml[100]
        self.ml.append(self.e6)
        self.assertEqual(self.e6, self.ml[self.e6.id])
    
    def test_setitem(self):
        self.ml[1] = self.e1
        self.assertIn(self.e1.id, self.ml)
        self.assertEqual(len(self.ml), 1)
        self.ml[1] = self.e3  # e1 and e3 has same id
        self.assertIn(self.e3.id, self.ml)
        self.assertEqual(self.ml[1].host, self.e3.host)

    def test_iter(self):
        for i in range(6):  # e1 to e6
            self.ml.append(getattr(self, f"e{i+1}"))
        for i, entry in enumerate(self.ml):
            self.assertEqual(getattr(self, f"e{i+1}"), entry)

    def test_pop(self):
        self.ml.append(self.e1)
        self.ml.append(self.e6)
        self.assertEqual(len(self.ml), 2)

        self.ml.pop(1)
        self.assertEqual(len(self.ml), 1)
        self.assertIn(self.e6.id, self.ml)
        self.assertEqual(self.ml[self.e6.id], self.e6)

        self.assertIsNone(self.ml.pop(100))
        self.assertEqual(len(self.ml), 1)

    def test_ml(self):
        # From previous mp2 commits
        ml = MembershipList()
        entry = _MLEntry(1, "local", 12345, 1.25)

        ml.append(entry)
        self.assertEqual(len(ml), 1)
        self.assertIsInstance(ml["1"], _MLEntry)
        self.assertEqual(ml["1"], entry)

        ml["1"]["port"] = 12345
        self.assertEqual(ml["1"]["port"], 12345)

        entry = _MLEntry(2, "abc", 234, 2.5)
        ml["2"] = entry
        self.assertEqual(len(ml), 2)
        self.assertEqual(ml["2"], entry)
        self.assertEqual(ml["2"].id, 2)

        entry.port = 200
        ml["2"] = entry
        self.assertEqual(ml["2"], entry)

        ml["0"] = _MLEntry(0, "bcd", 123, 3)
        i = 0
        for e in ml:
            self.assertEqual(e.id, i)
            i += 1

        # Test: __contains__
        self.assertIn("1", ml)
        self.assertNotIn("12345", ml)

        # Test: contains()
        self.assertTrue(ml.contains("bcd", "host"))
        self.assertTrue(ml.contains(123, "port"))
        self.assertTrue(ml.contains("123", "port"))
        self.assertTrue(ml.contains(0, "id"))
        with self.assertRaises(ValueError):
            ml.contains("abc", "type undefined")

        # Test: pop()
        ml = MembershipList()
        ml["1"] = _MLEntry(1, "abc", 123, 0)
        ml["0"] = _MLEntry(0, "bcd", 234, 1)
        ml["2"] = _MLEntry(2, "cde", 345, 2)
        entry = ml.pop("0")
        self.assertEqual(entry.id, 0)
        self.assertEqual(len(ml), 2)
        self.assertIn("1", ml)
        self.assertIn("2", ml)

        entry = ml.pop("2")
        self.assertEqual(entry.id, 2)
        self.assertEqual(len(ml), 1)
        self.assertIn("1", ml)

        ml = MembershipList()
        ml["1"] = _MLEntry(1, "abc", 123, 0)
        ml["0"] = _MLEntry(0, "bcd", 234, 1)
        ml["2"] = _MLEntry(2, "cde", 345, 2)
        entry = ml.pop("0")
        self.assertEqual(entry.id, 0)
        self.assertEqual(len(ml), 2)
        entry = ml.pop("0")
        self.assertIsNone(entry)
        self.assertEqual(len(ml), 2)

    def assert_id_in_neighbors(self, id: int, neighbors: List[_MLEntry]):
        id_in_neighbor = False
        for neighbor in neighbors:
            if neighbor.id == id:
                id_in_neighbor = True
        self.assertTrue(id_in_neighbor)

    def test_ml_neighors(self):
        # Super important method, critical for all other FD functionalities.

        # Test: ml.neighbor with > 4 entries
        for i in reversed(range(4 + 1)):  # [0, 1, 2, 3, 4]
            self.ml.append(_MLEntry(i, f"host_{i}", 1000 + i, 0))
        self.assertEqual(len(self.ml), 4 + 1)

        neighbors = self.ml.neighbors(2)  # [0, 1, 3, 4]
        self.assertEqual(len(neighbors), 4)
        self.assertEqual(neighbors[0].id, 0, "ml.neighbor(2) should be [0, 1, 3, 4]")
        self.assertEqual(neighbors[-1].id, 4)
        neighbors = self.ml.neighbors(2, include_self=True)  # [0, 1, 2, 3]
        self.assertEqual(len(neighbors), 4)
        self.assert_id_in_neighbors(2, neighbors)

        neighbors = self.ml.neighbors(3)  # [1, 2, 4, 0]
        self.assertEqual(len(neighbors), 4)
        self.assert_id_in_neighbors(1, neighbors)
        self.assert_id_in_neighbors(0, neighbors)
        self.assert_id_in_neighbors(4, neighbors)

        neighbors = self.ml.neighbors(4)  # neighbor(4) = [2, 3, 0, 1]
        self.assertEqual(len(neighbors), 4)
        self.assert_id_in_neighbors(0, neighbors)
        self.assert_id_in_neighbors(1, neighbors)
        self.assert_id_in_neighbors(2, neighbors)
        self.assert_id_in_neighbors(3, neighbors)
        neighbors = self.ml.neighbors(4, include_self=True)
        self.assertEqual(len(neighbors), 4)
        self.assert_id_in_neighbors(4, neighbors)

        # Test: ml.neighbor with 10 entries
        self.ml.clear()
        for i in reversed(range(10)):  # [0, 1, 2, ..., 8, 9]
            self.ml.append(_MLEntry(i, f"host_{i}", 1000 + i, 0))
        self.assertEqual(len(self.ml), 10)

        neighbors = self.ml.neighbors(9)  # [7, 8, 0, 1]
        self.assertEqual(len(neighbors), 4)
        self.assert_id_in_neighbors(1, neighbors)
        self.assert_id_in_neighbors(0, neighbors)
        self.assert_id_in_neighbors(7, neighbors)
        self.assert_id_in_neighbors(8, neighbors)


        # Test: ml.neighbor with < 4 entries
        self.ml.clear()
        self.ml.append(_MLEntry(0, "host_0", 1000, 0))
        neighbors = self.ml.neighbors(0)
        self.assertIsInstance(neighbors, list, "ml.neighbor should always return a list")
        self.assertEqual(len(neighbors), 0, "ml.neighbor might return an empty list")
        neighbors = self.ml.neighbors(0, include_self=True)
        self.assertEqual(len(neighbors), 1, "When include_self=True, at least one element in ml.neighbor.")
        self.assertEqual(neighbors[0].id, 0)

    def test_mid(self):
        # Test message.mid
        # Also super important
        m1 = Message(0, "abc", 123, 100.0, "TEST", content={"id": 234})
        m2 = Message(0, "abc", 123, 100.0, "TEST", content={"id": 345})
        self.assertTrue(m1.mid != m2.mid)

        m3 = Message(0, "abc", 123, 100.0, "TEST", content={"id": 234, "host": "bcd"})
        self.assertTrue(m1.mid != m3.mid)

        m4 = Message(0, "abc", 123, 100.0, "TEST", content={"host": "bcd", "id": 234})
        self.assertTrue(m3.mid == m4.mid)



class TestMsgCache(unittest.TestCase):

    def setUp(self) -> None:
        self.max_size = 5
        self.cache = _MsgCache(self.max_size)
        self.assertEqual(self.cache.max_size, self.max_size)

    def test_set(self):
        # Test: _MsgCache.set()
        self.assertEqual(len(self.cache._content), 0)

        mid = "example_mid_123"
        self.cache.set(mid, 12)
        self.assertIn(mid, self.cache._content)
        self.assertIn(mid, self.cache)  # test __contains__
        
        # Test: repetitive mid
        self.cache.set(mid, 13)  # same mid different target
        self.assertIn(mid, self.cache)
        self.assertEqual(len(self.cache._content), 1)
        self.assertEqual(len(self.cache._content[mid]), 2)
        self.cache.set(mid, 12)  # same mid same target
        self.assertIn(mid, self.cache)
        self.assertEqual(len(self.cache._content), 1)
        self.assertEqual(len(self.cache._content[mid]), 2)
        
        # Test: max size
        self.assertEqual(len(self.cache._content), 1)
        for i in range(self.max_size - 1):
            self.cache.set(f"{mid}_{i}", i)
        self.assertEqual(len(self.cache._content), self.max_size, "Cache should be full")
        self.assertIn(f"{mid}_0", self.cache)
        
        self.cache.set(f"{mid}_{self.max_size}", self.max_size)  # pop first
        self.assertEqual(len(self.cache._content), self.max_size, "Cache should still be full")
        self.assertNotIn(mid, self.cache, "First item should be poped")

    def test_sent(self):
        # Test: _MsgCache.sent()
        self.assertEqual(len(self.cache._content), 0)

        mid = "example_mid_123"
        self.cache.set(mid, 12)
        self.assertTrue(self.cache.sent(mid, 12))
        self.assertFalse(self.cache.sent(mid, 13))
        self.assertFalse(self.cache.sent("another_mid", 12))
        self.assertFalse(self.cache.sent("another_mid2", 14))
        
        mid = "example_mid_123_1"
        self.cache.set(mid, 13)
        self.assertTrue(self.cache.sent(mid, 13))
        self.assertFalse(self.cache.sent(mid, 12))


class TestFD(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.e1 = _MLEntry(1, "192", 123, 0.2)
        cls.e2 = _MLEntry(2, "123", 234, 0.3)
        cls.e3 = _MLEntry(3, "135", 100, 0.8)
        cls.e4 = _MLEntry(4, "987", 300, 1.25)
        cls.e5 = _MLEntry(5, "394", 400, 1.75)
        cls.logger = getLogger(__name__, "testfd_unit.log")
        
        cls.host = get_host()
        cls.port = 1234

    def setUp(self) -> None:
        self.fd = FailureDetector(0)
        self.fd.logger = self.logger

    def test_send_to(self):
        s = TestSocket(self.host, self.port)
        self.fd.ml.append(self.e1)
        self.fd.ml.append(self.e2)
        self.assertEqual(len(self.fd.ml), 2)
        
        message: Message = self.fd.generate_message("TEST", {"a": 123})
        self.fd.send_to(message, self.e1.id, s)

        self.assertTrue(self.fd.sent(message.mid, self.e1.id), "send_to should set message has sent to id.")
        self.assertIsNotNone(s.last_packet)
        last_message: Message = pickle.loads(s.last_packet)
        self.assertEqual(last_message.id, self.fd.id)
        self.assertEqual(last_message.host, self.fd.host)
        self.assertEqual(last_message.port, self.fd.port)
        self.assertEqual(s.last_packet_addr, ("192", 123), "Test if sendto address is correct.")
        self.assertEqual(last_message.content.get("a"), 123)
        self.assertEqual(last_message.message_type, "TEST")

        with self.assertLogs(self.fd.logger, self.fd.logger.level):
            self.fd.send_to(message, self.e2.id, s)
        self.assertTrue(self.fd.sent(message.mid, self.e2.id))

    def test_multicast(self):
        s = TestSocket(self.host, self.port)
        for i in range(5, 0, -1):
            self.fd.ml.append(getattr(self, f"e{i}"))
        self.assertEqual(len(self.fd.ml), 5)
        self.assertIn(3, self.fd.ml)

        self.fd.id = 3  # otherwise ml.neighbors would be incorect
        message: Message = self.fd.generate_message("TEST", {"b": 3})
        self.fd.multicast(message, s, sent_check=True)

        self.assertEqual(s.sendto_cnt, 4, "Should have sent to all 4 neighbors.")
        for i in range(5, 1, -1):  # id 2, 3, 4, 5
            self.fd.sent(message.mid, i)
