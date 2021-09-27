import logging
import sys
from collections.abc import Mapping, MutableMapping
from gzip import compress, decompress
from pathlib import Path
from typing import Any, Callable, Generic, Iterator, List, Tuple, TypeVar, Union

import wiredtiger

T = TypeVar("T")
KT = TypeVar("KT")
VT = TypeVar("VT")

logger = logging.getLogger(__name__)

_DEFAULT = object()


class error(Exception):
    pass


class _ConvertErrors:
    def __init__(self) -> None:
        self.funcs: List[Callable] = []

    def except_(self, func: Callable) -> None:
        self.funcs.append(func)

    def __enter__(self) -> "_ConvertErrors":
        return self

    def __exit__(self, exc_type, exc_value, traceback):

        if isinstance(exc_value, wiredtiger.WiredTigerError):
            msg = exc_value.args[0]
            if msg.startswith("WT_PANIC:"):
                sys.exit(msg)
            else:
                for func in self.funcs:
                    func()
                raise error(msg)


def remove_wtdbm(path):
    from shutil import rmtree

    rmtree(path)


class WiredTigerDBM(MutableMapping, Generic[KT, VT]):

    table_name = "wtdbm"

    def __init__(self, conn: wiredtiger.Connection, mode: str) -> None:
        self.conn = conn
        try:
            self.session = conn.open_session()
        except wiredtiger.WiredTigerError as e:
            ret = self.conn.close()
            assert ret == 0
            raise error(e.args[0])

        with _ConvertErrors() as e:
            if mode in ("r", "w"):
                if not self._initialized():
                    self.close()
                    raise error("Uninitialized database")
            elif mode == "c":
                self._create()
                e.except_(self.close)
            elif mode == "n":
                self._drop(True)
                self._create()
                e.except_(self.close)

    def _initialized(self):
        try:
            cursor = self.session.open_cursor(f"table:{self.table_name}", None, "overwrite=false")
        except wiredtiger.WiredTigerError as e:
            if e.args[0] == "No such file or directory":
                return False
        else:
            cursor.close()
            return True

    def _create(self) -> None:
        ret = self.session.create(f"table:{self.table_name}", "key_format=u,value_format=u,exclusive=false")
        assert ret == 0

    def _drop(self, force: bool = False) -> None:
        if force:
            config = "force=true"
        else:
            config = "force=false"

        ret = self.session.drop(f"table:{self.table_name}", config)
        assert ret == 0

    @classmethod
    def open(cls, file: str, mode: str = "r") -> "WiredTigerDBM":

        if mode == "r":
            config = "readonly"
        elif mode == "w":
            config = ""
        elif mode == "c":
            Path(file).mkdir(parents=True, exist_ok=True)
            config = "create"
        elif mode == "n":
            config = "create"
        # elif mode == "x": # non-standard
        #    config = "create,exclusive"
        else:
            raise ValueError("Invalid mode")

        logger.debug("Using %s", wiredtiger.wiredtiger_version()[0])

        with _ConvertErrors():
            conn = wiredtiger.wiredtiger_open(file, config + ",log=(enabled=false),verbose=[]")

        return cls(conn, mode)

    def _pre_key(self, key: KT) -> bytes:

        if isinstance(key, bytes):
            return key
        elif isinstance(key, str):
            return key.encode("Latin-1")

        raise TypeError(key)

    def _post_key(self, key: bytes) -> KT:

        return key

    def _pre_value(self, value: VT) -> bytes:

        if isinstance(value, bytes):
            return value
        elif isinstance(value, str):
            return value.encode("Latin-1")

        raise TypeError(value)

    def _post_value(self, value: bytes) -> VT:

        return value

    def __getitem__(self, key: KT) -> VT:

        cursor = self.session.open_cursor(f"table:{self.table_name}", None, "overwrite=false")
        try:
            cursor.set_key(self._pre_key(key))
            ret = cursor.search()
            if ret == 0:
                value = cursor.get_value()
            elif ret == wiredtiger.WT_NOTFOUND:
                raise KeyError(f"{key} not found")
            else:
                raise error(f"Unknown return code: {ret}")
            return self._post_value(value)
        finally:
            cursor.close()

    def __setitem__(self, key: KT, value: VT) -> None:

        cursor = self.session.open_cursor(f"table:{self.table_name}", None, "overwrite=true")
        try:
            cursor.set_key(self._pre_key(key))
            cursor.set_value(self._pre_value(value))
            ret = cursor.insert()
            if ret == 0:
                pass
            else:
                raise error(f"Unknown return code: {ret}")
        except wiredtiger.WiredTigerError as e:
            raise error(e.args[0])
        finally:
            cursor.close()

    def __delitem__(self, key: KT) -> None:

        cursor = self.session.open_cursor(f"table:{self.table_name}", None, "overwrite=false")
        try:
            cursor.set_key(self._pre_key(key))
            ret = cursor.remove()
            if ret == 0:
                pass
            elif ret == wiredtiger.WT_NOTFOUND:
                raise KeyError(f"{key} not found")
            else:
                raise error(f"Unknown return code: {ret}")
        finally:
            cursor.close()

    def keys(self) -> Iterator[KT]:

        cursor = self.session.open_cursor(f"table:{self.table_name}", None, "overwrite=false")
        try:
            while True:
                ret = cursor.next()
                if ret == 0:
                    yield self._post_key(cursor.get_key())
                elif ret == wiredtiger.WT_NOTFOUND:
                    break
                else:
                    raise error(f"Unknown return code: {ret}")
        finally:
            cursor.close()

    def items(self) -> Iterator[Tuple[KT, VT]]:

        cursor = self.session.open_cursor(f"table:{self.table_name}", None, "overwrite=false")
        try:
            while True:
                ret = cursor.next()
                if ret == 0:
                    k = cursor.get_key()
                    v = cursor.get_value()
                    yield self._post_key(k), self._post_value(v)
                elif ret == wiredtiger.WT_NOTFOUND:
                    break
                else:
                    raise error(f"Unknown return code: {ret}")
        finally:
            cursor.close()

    def values(self) -> Iterator[VT]:

        cursor = self.session.open_cursor(f"table:{self.table_name}", None, "overwrite=false")
        try:
            while True:
                ret = cursor.next()
                if ret == 0:
                    yield self._post_value(cursor.get_value())
                elif ret == wiredtiger.WT_NOTFOUND:
                    break
                else:
                    raise error(f"Unknown return code: {ret}")
        finally:
            cursor.close()

    def __contains__(self, key: KT) -> bool:

        cursor = self.session.open_cursor(f"table:{self.table_name}", None, "overwrite=false")
        try:
            cursor.set_key(self._pre_key(key))
            ret = cursor.search()
            if ret == 0:
                return True
            elif ret == wiredtiger.WT_NOTFOUND:
                return False
            else:
                raise error(f"Unknown return code: {ret}")
        finally:
            cursor.close()

    def __iter__(self) -> Iterator[KT]:

        return self.keys()

    def __len__(self) -> int:

        raise RuntimeError("Not implemented yet")

    def pop(self, key: KT, default: Union[VT, T] = _DEFAULT) -> Union[VT, T]:

        cursor = self.session.open_cursor(f"table:{self.table_name}", None, "overwrite=false")
        try:
            cursor.set_key(self._pre_key(key))
            ret = cursor.search()
            if ret == 0:
                value = cursor.get_value()
                ret = cursor.remove()
                if ret == 0:
                    pass
                else:
                    raise error(f"remove failed: {ret}")
            elif ret == wiredtiger.WT_NOTFOUND:
                return default
            else:
                raise error(f"Unknown return code: {ret}")
            return self._post_value(value)
        finally:
            cursor.close()

    def update(self, __other: Any = (), **kwds: VT) -> None:

        cursor = self.session.open_cursor(f"table:{self.table_name}", None, "overwrite=true")
        try:
            self.session.begin_transaction()
            if isinstance(__other, Mapping):
                for key in __other:
                    cursor.set_key(self._pre_key(key))
                    cursor.set_value(self._pre_value(__other[key]))
                    ret = cursor.insert()
                    assert ret == 0
            elif hasattr(__other, "keys"):
                for key in __other.keys():
                    cursor.set_key(self._pre_key(key))
                    cursor.set_value(self._pre_value(__other[key]))
                    ret = cursor.insert()
                    assert ret == 0
            else:
                for key, value in __other:
                    cursor.set_key(self._pre_key(key))
                    cursor.set_value(self._pre_value(value))
                    ret = cursor.insert()
                    assert ret == 0
        except wiredtiger.WiredTigerError as e:
            self.session.rollback_transaction()
            raise error(e.args[0])
        else:
            self.session.commit_transaction()
        finally:
            cursor.close()

    def get(self, key: KT, default: Union[VT, T] = _DEFAULT) -> Union[VT, T]:

        cursor = self.session.open_cursor(f"table:{self.table_name}", None, "overwrite=false")
        try:
            cursor.set_key(self._pre_key(key))
            ret = cursor.search()
            if ret == 0:
                value = cursor.get_value()
            elif ret == wiredtiger.WT_NOTFOUND:
                return default
            else:
                raise error(f"Unknown return code: {ret}")
            return self._post_value(value)
        finally:
            cursor.close()

    def setdefault(self, key: KT, default: VT) -> VT:
        cursor = self.session.open_cursor(f"table:{self.table_name}", None, "overwrite=false")
        try:
            cursor.set_key(self._pre_key(key))
            ret = cursor.search()
            if ret == 0:
                return self._post_value(cursor.get_value())
            elif ret == wiredtiger.WT_NOTFOUND:
                cursor.set_value(self._pre_value(default))
                ret = cursor.insert()
                if ret == 0:
                    return default
                else:
                    raise error(f"Set failed: {ret}")
            else:
                raise error(f"Get failed: {ret}")
        except wiredtiger.WiredTigerError as e:
            raise error(e.args[0])
        finally:
            cursor.close()

    def sync(self) -> None:

        raise RuntimeError("Not implemented yet")

    def clear(self) -> None:
        with _ConvertErrors():
            self._drop()
            self._create()

    def close(self) -> None:

        self.session.close()
        ret = self.conn.close()  # Very important. To actually write data to file, it's not enough to close the session.
        assert ret == 0

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()


class WiredTigerDBMGzip(WiredTigerDBM):
    def __init__(self, conn: wiredtiger.Connection, mode: str, compresslevel: int = 9):
        WiredTigerDBM.__init__(self, conn, mode)
        self.compresslevel = compresslevel

    def _pre_value(self, value: VT) -> bytes:

        value = WiredTigerDBM._pre_value(self, value)
        return compress(value, self.compresslevel)

    def _post_value(self, value: bytes) -> VT:

        return decompress(value)


def open(file, flag="r"):
    return WiredTigerDBM.open(file, flag)
