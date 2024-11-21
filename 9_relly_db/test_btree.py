import os
import tempfile
import struct
from buffer import BufferPool, BufferPoolManager
from disk import DiskManager
from btree import BTree, SearchMode

def test_btree():
    with tempfile.NamedTemporaryFile(delete=False) as temp_file:
        temp_file_path = temp_file.name

    try:
        disk = DiskManager.open(temp_file_path)
        pool = BufferPool(10)
        bufmgr = BufferPoolManager(disk, pool)
        btree = BTree.create(bufmgr)

        btree.insert(bufmgr, struct.pack('>Q', 6), b"world")
        btree.insert(bufmgr, struct.pack('>Q', 3), b"hello")
        btree.insert(bufmgr, struct.pack('>Q', 8), b"!")
        btree.insert(bufmgr, struct.pack('>Q', 4), b",")

        iter = btree.search(bufmgr, SearchMode(SearchMode.KEY, struct.pack('>Q', 3)))
        key, value = iter.get()
        assert struct.unpack('>Q', key)[0] == 3
        assert value == b"hello"

        iter = btree.search(bufmgr, SearchMode(SearchMode.KEY, struct.pack('>Q', 8)))
        key, value = iter.get()
        assert struct.unpack('>Q', key)[0] == 8
        assert value == b"!"

    finally:
        os.remove(temp_file_path)
