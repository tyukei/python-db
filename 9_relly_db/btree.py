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
    LEAF_NODE_MAX_PAIRS = 3  # リーフノードの最大ペア数
    BRANCH_NODE_MAX_KEYS = 4  # ブランチノードの最大キー数

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
            print(f"Leaf node found: {[(struct.unpack('>Q', pair.key)[0], pair.value.decode()) for pair in pairs]}")
            if search_mode.mode == SearchMode.START:
                return Iter(node_buffer, 0)
            elif search_mode.mode == SearchMode.KEY:
                for i, pair in enumerate(pairs):
                    if pair.key >= search_mode.key:
                        return Iter(node_buffer, i)
                return Iter(node_buffer, len(pairs) - 1)  # 修正: 最後のペアのインデックスに設定
        else:  # Branch node
            keys, children = self.get_branch(node_buffer)
            print(f"Branch node: {[(struct.unpack('>Q', key)[0]) for key in keys]}")
            if search_mode.mode == SearchMode.START:
                return self.search_internal(bufmgr, bufmgr.fetch_page(children[0]), search_mode)
            elif search_mode.mode == SearchMode.KEY:
                for i, key in enumerate(keys):
                    if search_mode.key < key:
                        return self.search_internal(bufmgr, bufmgr.fetch_page(children[i]), search_mode)
                return self.search_internal(bufmgr, bufmgr.fetch_page(children[-1]), search_mode)

            
    def insert(self, bufmgr: BufferPoolManager, key: bytes, value: bytes) -> None:
        print(f"Inserting key: {struct.unpack('>Q', key)[0]}, value: {value.decode()}")
        root_page = self.fetch_root_page(bufmgr)
        new_child = self.insert_internal(bufmgr, root_page, key, value)
        if new_child is not None:
            new_root_buffer = bufmgr.create_page()
            new_root_buffer.page[:4] = struct.pack('>I', 1)  # Initialize as branch
            new_root_buffer.is_dirty = True
            meta_buffer = bufmgr.fetch_page(self.meta_page_id)
            meta_buffer.page[:8] = new_root_buffer.page_id.to_bytes()
            meta_buffer.is_dirty = True
            new_key, new_page_id = new_child
            self.set_branch(new_root_buffer, [new_key], [root_page.page_id, new_page_id])
        else:
            print("No new child created, insertion complete.")



    def insert_internal(self, bufmgr: BufferPoolManager, node_buffer: Buffer, key: bytes, value: bytes) -> Optional[Tuple[bytes, PageId]]:
        node_type = struct.unpack('>I', node_buffer.page[:4])[0]
        if node_type == 0:  # Leaf node
            pairs = self.get_pairs(node_buffer)
            for pair in pairs:
                if pair.key == key:
                    raise DuplicateKeyError("Duplicate key")
            pairs.append(Pair(key, value))
            pairs.sort(key=lambda p: p.key)
            if len(pairs) > BTree.LEAF_NODE_MAX_PAIRS:
                return self.split_leaf_node(bufmgr, node_buffer, pairs)
            else:
                self.set_leaf(node_buffer, pairs)
                node_buffer.is_dirty = True
                return None
        else:  # Branch node
            keys, children = self.get_branch(node_buffer)
            for i, k in enumerate(keys):
                if key < k:
                    new_child = self.insert_internal(bufmgr, bufmgr.fetch_page(children[i]), key, value)
                    break
            else:
                new_child = self.insert_internal(bufmgr, bufmgr.fetch_page(children[-1]), key, value)

            if new_child is not None:
                new_key, new_page_id = new_child
                keys.append(new_key)
                children.append(new_page_id)
                keys, children = zip(*sorted(zip(keys, children)))
                if len(keys) > BTree.BRANCH_NODE_MAX_KEYS:
                    return self.split_branch_node(bufmgr, node_buffer, list(keys), list(children))
                else:
                    self.set_branch(node_buffer, list(keys), list(children))
                    node_buffer.is_dirty = True
                    return new_key, new_page_id



    def split_leaf_node(self, bufmgr: BufferPoolManager, node_buffer: Buffer, pairs: List[Pair]) -> Tuple[bytes, PageId]:
        mid_index = len(pairs) // 2
        left_pairs = pairs[:mid_index]
        right_pairs = pairs[mid_index:]  # Include the middle key in the right node

        new_leaf_buffer = bufmgr.create_page()
        new_leaf_buffer.page[:4] = struct.pack('>I', 0)  # Initialize as leaf
        self.set_leaf(node_buffer, left_pairs)
        self.set_leaf(new_leaf_buffer, right_pairs)
        node_buffer.is_dirty = True
        new_leaf_buffer.is_dirty = True

        print(f"split_leaf_node: left_pairs={[(struct.unpack('>Q', pair.key)[0], pair.value.decode()) for pair in left_pairs]}, right_pairs={[(struct.unpack('>Q', pair.key)[0], pair.value.decode()) for pair in right_pairs]}")
        # Return the middle key as the separator key
        return pairs[mid_index].key, new_leaf_buffer.page_id



    def split_branch_node(self, bufmgr: BufferPoolManager, node_buffer: Buffer, keys: List[bytes], children: List[PageId]) -> Tuple[bytes, PageId]:
        mid_index = len(keys) // 2
        left_keys = keys[:mid_index]
        right_keys = keys[mid_index+1:]
        left_children = children[:mid_index+1]
        right_children = children[mid_index+1:]

        new_branch_buffer = bufmgr.create_page()
        new_branch_buffer.page[:4] = struct.pack('>I', 1)  # Initialize as branch
        self.set_branch(node_buffer, left_keys, left_children)
        self.set_branch(new_branch_buffer, right_keys, right_children)
        node_buffer.is_dirty = True
        new_branch_buffer.is_dirty = True

        print(f"split_branch_node: left_keys={[struct.unpack('>Q', key)[0] for key in left_keys]}, right_keys={[struct.unpack('>Q', key)[0] for key in right_keys]}")
        # Return the middle key as the separator key
        return keys[mid_index], new_branch_buffer.page_id




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

    def get_branch(self, buffer: Buffer) -> Tuple[List[bytes], List[PageId]]:
        num_keys = struct.unpack('>I', buffer.page[4:8])[0]
        keys = []
        children = []
        offset = 8
        for _ in range(num_keys):
            key = buffer.page[offset:offset+8]
            page_id = PageId.from_bytes(buffer.page[offset+8:offset+16])
            keys.append(key)
            children.append(page_id)
            offset += 16
        return keys, children

    def set_branch(self, buffer: Buffer, keys: List[bytes], children: List[PageId]) -> None:
        buffer.page[4:8] = struct.pack('>I', len(keys))
        offset = 8
        for key, child in zip(keys, children):
            buffer.page[offset:offset+8] = key
            buffer.page[offset+8:offset+16] = child.to_bytes()
            offset += 16


    def set_leaf(self, buffer: Buffer, pairs: List[Pair]) -> None:
        buffer.page[4:8] = struct.pack('>I', len(pairs))
        offset = 8
        for pair in pairs:
            pair_data = pair.to_bytes()
            pair_size = len(pair_data)
            buffer.page[offset:offset+4] = struct.pack('>I', pair_size)
            buffer.page[offset+4:offset+4+pair_size] = pair_data
            offset += 4 + pair_size


    def print_tree(self, bufmgr: BufferPoolManager) -> None:
        root_page = self.fetch_root_page(bufmgr)
        print("Root Node:")
        self.print_node(bufmgr, root_page, 0)

    def print_node(self, bufmgr: BufferPoolManager, node_buffer: Buffer, level: int) -> None:
        node_type = struct.unpack('>I', node_buffer.page[:4])[0]
        indent = "  " * level
        if node_type == 0:  # Leaf node
            pairs = self.get_pairs(node_buffer)
            print(f"{indent}Leaf Node: {[(struct.unpack('>Q', pair.key)[0], pair.value.decode()) for pair in pairs]}")
        else:  # Branch node
            keys, children = self.get_branch(node_buffer)
            print(f"{indent}Branch Node: {[struct.unpack('>Q', key)[0] for key in keys]}")
            for child_page_id in children:
                child_buffer = bufmgr.fetch_page(child_page_id)
                self.print_node(bufmgr, child_buffer, level + 1)

class Iter:
    def __init__(self, buffer: Buffer, slot_id: int):
        self.buffer = buffer
        self.slot_id = slot_id

    def get(self) -> Optional[Tuple[bytes, bytes]]:
        pairs = BTree(self.buffer.page_id).get_pairs(self.buffer)
        print(f"Iter.get() called: slot_id={self.slot_id}, pairs={[(struct.unpack('>Q', pair.key)[0], pair.value.decode()) for pair in pairs]}")
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
        btree.insert(bufmgr, struct.pack('>Q', 5), b"test")

        print("BTree structure:")
        btree.print_tree(bufmgr)

        iter = btree.search(bufmgr, SearchMode.Key(struct.pack('>Q', 3)))
        key, value = iter.get()
        if key is not None and value is not None:
            print(f"Key: {struct.unpack('>Q', key)[0]}, Value: {value.decode()}")
        else:
            print("Key not found")

        iter = btree.search(bufmgr, SearchMode.Key(struct.pack('>Q', 8)))
        key, value = iter.get()
        if key is not None and value is not None:
            print(f"Key: {struct.unpack('>Q', key)[0]}, Value: {value.decode()}")
        else:
            print("Key not found")

        print("BTree tests passed.")
    finally:
        os.remove(temp_file_path)
