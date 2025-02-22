from btree import BTree
from buffer import BufferPoolManager
from disk import PageId
import tuple

class SimpleTable:
    def __init__(self, meta_page_id: PageId = None, num_key_elems: int = 0):
        self.meta_page_id = meta_page_id
        self.num_key_elems = num_key_elems

    def create(self, bufmgr: BufferPoolManager) -> None:
        btree = BTree.create(bufmgr)
        self.meta_page_id = btree.meta_page_id

    def insert(self, bufmgr: BufferPoolManager, record: list[list[bytes]]) -> None:
        btree = BTree(self.meta_page_id)
        key = []
        tuple.encode(record[:self.num_key_elems], key)
        value = []
        tuple.encode(record[self.num_key_elems:], value)
        btree.insert(bufmgr, key, value)

class UniqueIndex:
    def __init__(self, meta_page_id: PageId = None, skey: list[int] = None):
        self.meta_page_id = meta_page_id
        self.skey = skey or []

    def create(self, bufmgr: BufferPoolManager) -> None:
        btree = BTree.create(bufmgr)
        self.meta_page_id = btree.meta_page_id

    def insert(self, bufmgr: BufferPoolManager, pkey: bytes, record: list[bytes]) -> None:
        btree = BTree(self.meta_page_id)
        skey = []
        tuple.encode([record[index] for index in self.skey], skey)
        btree.insert(bufmgr, skey, pkey)

class Table:
    def __init__(self, meta_page_id: PageId = None, num_key_elems: int = 0, unique_indices: list[UniqueIndex] = None):
        self.meta_page_id = meta_page_id
        self.num_key_elems = num_key_elems
        self.unique_indices = unique_indices or []

    def create(self, bufmgr: BufferPoolManager) -> None:
        btree = BTree.create(bufmgr)
        self.meta_page_id = btree.meta_page_id
        for unique_index in self.unique_indices:
            unique_index.create(bufmgr)

    def insert(self, bufmgr: BufferPoolManager, record: list[list[bytes]]) -> None:
        btree = BTree(self.meta_page_id)
        key = []
        tuple.encode(record[:self.num_key_elems], key)
        value = []
        tuple.encode(record[self.num_key_elems:], value)
        btree.insert(bufmgr, key, value)
        for unique_index in self.unique_indices:
            unique_index.insert(bufmgr, key, record)
