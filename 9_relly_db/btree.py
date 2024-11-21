import struct
from typing import Optional, Tuple, List
from buffer import BufferPoolManager, Buffer
from disk import PageId, PAGE_SIZE
import pickle
import os

class BTreeError(Exception):
    pass

class DuplicateKeyError(BTreeError):
    pass

class SearchMode:
    START = 0
    KEY = 1

    def __init__(self, mode: int, key: Optional[bytes] = None):
        self.mode = mode
        self.key = key

    @staticmethod
    def Start():
        return SearchMode(SearchMode.START)

    @staticmethod
    def Key(key: bytes):
        return SearchMode(SearchMode.KEY, key)

class Pair:
    def __init__(self, key: bytes, value: bytes):
        self.key = key
        self.value = value

    def to_bytes(self) -> bytes:
        return pickle.dumps(self)

    @staticmethod
    def from_bytes(data: bytes) -> 'Pair':
        return pickle.loads(data)

class BTree:
    def __init__(self, meta_page_id: PageId):
        self.meta_page_id = meta_page_id

    @staticmethod
    def create(bufmgr: BufferPoolManager) -> 'BTree':
        meta_buffer = bufmgr.create_page()
        root_buffer = bufmgr.create_page()
        root_buffer.page[:4] = struct.pack('>I', 0)  # Initialize as leaf
        meta_buffer.page[:8] = root_buffer.page_id.to_bytes()
        meta_buffer.is_dirty = True
        root_buffer.is_dirty = True
        return BTree(meta_buffer.page_id)

    def fetch_root_page(self, bufmgr: BufferPoolManager) -> Buffer:
        meta_buffer = bufmgr.fetch_page(self.meta_page_id)
        root_page_id = PageId.from_bytes(meta_buffer.page[:8])
        return bufmgr.fetch_page(root_page_id)

    def search(self, bufmgr: BufferPoolManager, search_mode: SearchMode) -> 'Iter':
        root_page = self.fetch_root_page(bufmgr)
        return self.search_internal(bufmgr, root_page, search_mode)

    def search_internal(self, bufmgr: BufferPoolManager, node_buffer: Buffer, search_mode: SearchMode) -> 'Iter':
        node_type = struct.unpack('>I', node_buffer.page[:4])[0]
        if node_type == 0:  # Leaf node
            pairs = self.get_pairs(node_buffer)
            if search_mode.mode == SearchMode.START:
                return Iter(node_buffer, 0)
            elif search_mode.mode == SearchMode.KEY:
                for i, pair in enumerate(pairs):
                    if pair.key >= search_mode.key:
                        return Iter(node_buffer, i)
                return Iter(node_buffer, len(pairs))
        else:  # Branch node
            raise NotImplementedError("Branch node search not implemented")

    def insert(self, bufmgr: BufferPoolManager, key: bytes, value: bytes) -> None:
        print(f"Inserting key: {struct.unpack('>Q', key)[0]}, value: {value.decode()}")
        root_page = self.fetch_root_page(bufmgr)
        if not self.insert_internal(bufmgr, root_page, key, value):
            new_root_buffer = bufmgr.create_page()
            new_root_buffer.page[:4] = struct.pack('>I', 1)  # Initialize as branch
            new_root_buffer.is_dirty = True
            meta_buffer = bufmgr.fetch_page(self.meta_page_id)
            meta_buffer.page[:8] = new_root_buffer.page_id.to_bytes()
            meta_buffer.is_dirty = True

    def insert_internal(self, bufmgr: BufferPoolManager, node_buffer: Buffer, key: bytes, value: bytes) -> bool:
        node_type = struct.unpack('>I', node_buffer.page[:4])[0]
        if node_type == 0:  # Leaf node
            pairs = self.get_pairs(node_buffer)
            for pair in pairs:
                if pair.key == key:
                    raise DuplicateKeyError("Duplicate key")
            pairs.append(Pair(key, value))
            pairs.sort(key=lambda p: p.key)
            self.set_pairs(node_buffer, pairs)
            node_buffer.is_dirty = True
            return True
        else:  # Branch node
            raise NotImplementedError("Branch node insert not implemented")

    def get_pairs(self, buffer: Buffer) -> List[Pair]:
        num_pairs = struct.unpack('>I', buffer.page[4:8])[0]
        pairs = []
        offset = 8
        for _ in range(num_pairs):
            pair_size = struct.unpack('>I', buffer.page[offset:offset+4])[0]
            pair_data = buffer.page[offset+4:offset+4+pair_size]
            pairs.append(Pair.from_bytes(pair_data))
            offset += 4 + pair_size
        return pairs

    def set_pairs(self, buffer: Buffer, pairs: List[Pair]) -> None:
        buffer.page[4:8] = struct.pack('>I', len(pairs))
        offset = 8
        for pair in pairs:
            pair_data = pair.to_bytes()
            pair_size = len(pair_data)
            buffer.page[offset:offset+4] = struct.pack('>I', pair_size)
            buffer.page[offset+4:offset+4+pair_size] = pair_data
            offset += 4 + pair_size

class Iter:
    def __init__(self, buffer: Buffer, slot_id: int):
        self.buffer = buffer
        self.slot_id = slot_id

    def get(self) -> Optional[Tuple[bytes, bytes]]:
        pairs = BTree(self.buffer.page_id).get_pairs(self.buffer)
        if self.slot_id < len(pairs):
            pair = pairs[self.slot_id]
            return pair.key, pair.value
        return None

    def advance(self, bufmgr: BufferPoolManager) -> None:
        self.slot_id += 1

    def next(self, bufmgr: BufferPoolManager) -> Optional[Tuple[bytes, bytes]]:
        value = self.get()
        self.advance(bufmgr)
        return value

# テスト用コード
if __name__ == "__main__":
    import tempfile
    from buffer import BufferPool, BufferPoolManager
    from disk import DiskManager

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

        iter = btree.search(bufmgr, SearchMode.Key(struct.pack('>Q', 3)))
        key, value = iter.get()
        print(f"Key: {struct.unpack('>Q', key)[0]}, Value: {value.decode()}")

        iter = btree.search(bufmgr, SearchMode.Key(struct.pack('>Q', 8)))
        key, value = iter.get()
        print(f"Key: {struct.unpack('>Q', key)[0]}, Value: {value.decode()}")

        print("BTree tests passed.")
    finally:
        os.remove(temp_file_path)
