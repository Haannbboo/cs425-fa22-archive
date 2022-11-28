import unittest


from src.sdfs.sdfs import FileTable


class TestFileTable(unittest.TestCase):
    def test_file_table(self):
        ft = FileTable()
        self.assertIsNone(ft.get("abc"))  # get empty file

        ft.insert("abc.txt", set([1, 2, 3]), 1)
        self.assertIn("abc.txt", ft)
        file = ft.get("abc.txt")
        self.assertEqual(file.version, 1)

        ft.insert("abc.txt", set([2, 3, 4]), 2)
        self.assertEqual(len(ft.files.get("abc.txt")), 2)
        file = ft.get("abc.txt")
        self.assertEqual(file.version, 2)
        file = ft.get("abc.txt", version=1)
        self.assertIsNotNone(file)
        self.assertEqual(file.version, 1)

        ft.insert("abc.txt", set([3, 4, 5]), 1)
        file = ft.get("abc.txt", version=1)
        self.assertEqual(file.replicas, set([3, 4, 5]))

        ft.delete("abc.txt")
        self.assertNotIn("abc.txt", ft)
