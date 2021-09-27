from __future__ import generator_stop

from genutility.test import MyTestCase

from wtdbm import WiredTigerDBM, error


class WtdbmTests(MyTestCase):

    _name = "./test.db"

    def test_readonly(self):
        with self.assertRaises(error):
            with WiredTigerDBM.open(self._name, "r") as db:
                db[b"a"] = b"a"

        with self.assertRaises(error):
            with WiredTigerDBM.open(self._name, "r") as db:
                db.clear()

    def test_full(self):
        with WiredTigerDBM.open(self._name, "n") as db:
            key1 = b"asd1"
            value1 = b"qwe1"
            key2 = b"asd2"
            value2 = b"qwe2"
            key3 = b"asd3"
            value3 = b"qwe3"

            db[key1] = value1
            db[key2] = value2
            assert db[key1] == value1
            assert db.get(key1) == value1
            with self.assertRaises(KeyError):
                db[value1]
            assert db.get(value1, None) is None
            assert key1 in db
            assert value1 not in db

            del db[key1]
            assert key1 not in db
            db[key1] = value1
            assert key1 in db

            assert db.pop(key2) == value2
            assert db.pop(key2, None) is None

            assert list(db.keys()) == [key1], list(db.keys())
            assert list(db.values()) == [value1]
            assert list(db.items()) == [(key1, value1)]

            db.update([(key2, value2)])
            assert list(db.items()) == [(key1, value1), (key2, value2)]

            assert db.setdefault(key3, value3) == value3
            assert db.setdefault(key3, value1) == value3

            # db.update((key2, value2) for i in range(10000000)) # wtdbm.error: WT_ROLLBACK: conflict between concurrent operations


if __name__ == "__main__":
    import unittest

    unittest.main()
