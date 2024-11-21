import os
import struct
from collections import defaultdict
from typing import Dict, Optional, Tuple
from disk import DiskManager, PageId, PAGE_SIZE

class BufferError(Exception):
    pass

class NoFreeBufferError(BufferError):
    pass

class BufferId:
    def __init__(self, buffer_id: int):
        self.buffer_id = buffer_id

    def __eq__(self, other):
        if isinstance(other, BufferId):
            return self.buffer_id == other.buffer_id
        return False

    def __hash__(self):
        return hash(self.buffer_id)

    def __repr__(self):
        return f"BufferId({self.buffer_id})"

class Buffer:
    def __init__(self, page_id: PageId):
        self.page_id = page_id
        self.page = bytearray(PAGE_SIZE)
        self.is_dirty = False

class Frame:
    def __init__(self, buffer: Buffer):
        self.usage_count = 0
        self.buffer = buffer

class BufferPool:
    def __init__(self, pool_size: int):
        self.buffers = [Frame(Buffer(PageId(PageId.INVALID_PAGE_ID))) for _ in range(pool_size)]
        self.next_victim_id = BufferId(0)

    def size(self) -> int:
        return len(self.buffers)

    def evict(self) -> Optional[BufferId]:
        pool_size = self.size()
        consecutive_pinned = 0
        while True:
            frame = self.buffers[self.next_victim_id.buffer_id]
            if frame.usage_count == 0:
                return self.next_victim_id
            if frame.buffer.is_dirty:
                frame.usage_count -= 1
                consecutive_pinned = 0
            else:
                consecutive_pinned += 1
                if consecutive_pinned >= pool_size:
                    return None
            self.next_victim_id = BufferId((self.next_victim_id.buffer_id + 1) % pool_size)

class BufferPoolManager:
    def __init__(self, disk: DiskManager, pool: BufferPool):
        self.disk = disk
        self.pool = pool
        self.page_table: Dict[PageId, BufferId] = {}

    def fetch_page(self, page_id: PageId) -> Buffer:
        if page_id in self.page_table:
            buffer_id = self.page_table[page_id]
            frame = self.pool.buffers[buffer_id.buffer_id]
            frame.usage_count += 1
            return frame.buffer

        buffer_id = self.pool.evict()
        if buffer_id is None:
            raise NoFreeBufferError("No free buffer available in buffer pool")

        frame = self.pool.buffers[buffer_id.buffer_id]
        evict_page_id = frame.buffer.page_id

        if frame.buffer.is_dirty:
            self.disk.write_page_data(evict_page_id, frame.buffer.page)

        frame.buffer.page_id = page_id
        frame.buffer.is_dirty = False
        self.disk.read_page_data(page_id, frame.buffer.page)
        frame.usage_count = 1

        self.page_table.pop(evict_page_id, None)
        self.page_table[page_id] = buffer_id

        return frame.buffer

    def create_page(self) -> Buffer:
        buffer_id = self.pool.evict()
        if buffer_id is None:
            raise NoFreeBufferError("No free buffer available in buffer pool")

        frame = self.pool.buffers[buffer_id.buffer_id]
        evict_page_id = frame.buffer.page_id

        if frame.buffer.is_dirty:
            self.disk.write_page_data(evict_page_id, frame.buffer.page)

        page_id = self.disk.allocate_page()
        frame.buffer = Buffer(page_id)
        frame.buffer.is_dirty = True
        frame.usage_count = 1

        self.page_table.pop(evict_page_id, None)
        self.page_table[page_id] = buffer_id

        return frame.buffer

    def flush(self) -> None:
        print("Flushing buffers to disk...")
        for page_id, buffer_id in self.page_table.items():
            frame = self.pool.buffers[buffer_id.buffer_id]
            if frame.buffer.is_dirty:
                print(f"Flushing page {page_id.page_id} to disk")
                self.disk.write_page_data(page_id, frame.buffer.page)
                frame.buffer.is_dirty = False
        self.disk.sync()

# テスト用コード
if __name__ == "__main__":
    import tempfile

    with tempfile.NamedTemporaryFile(delete=False) as temp_file:
        temp_file_path = temp_file.name

    try:
        disk = DiskManager.open(temp_file_path)
        pool = BufferPool(1000)  # バッファプールのサイズを増やす
        bufmgr = BufferPoolManager(disk, pool)

        hello = bytearray(b"hello" + b"\x00" * (PAGE_SIZE - 5))
        world = bytearray(b"world" + b"\x00" * (PAGE_SIZE - 5))

        buffer1 = bufmgr.create_page()
        buffer1.page[:5] = b"hello"
        buffer1.is_dirty = True
        page1_id = buffer1.page_id

        buffer2 = bufmgr.create_page()
        buffer2.page[:5] = b"world"
        buffer2.is_dirty = True
        page2_id = buffer2.page_id

        bufmgr.flush()

        buffer1_read = bufmgr.fetch_page(page1_id)
        assert buffer1_read.page[:5] == b"hello"

        buffer2_read = bufmgr.fetch_page(page2_id)
        assert buffer2_read.page[:5] == b"world"

        print("BufferPoolManager tests passed.")
    finally:
        os.remove(temp_file_path)
