import os
import struct
from typing import Optional

PAGE_SIZE = 4096

class PageId:
    INVALID_PAGE_ID = 2**64 - 1

    def __init__(self, page_id: int):
        self.page_id = page_id

    def valid(self) -> Optional['PageId']:
        if self.page_id == PageId.INVALID_PAGE_ID:
            return None
        return self

    def to_u64(self) -> int:
        return self.page_id

    @staticmethod
    def from_bytes(bytes_data: bytes) -> 'PageId':
        page_id, = struct.unpack('Q', bytes_data)
        return PageId(page_id)

    def to_bytes(self) -> bytes:
        return struct.pack('Q', self.page_id)

    def __eq__(self, other):
        if isinstance(other, PageId):
            return self.page_id == other.page_id
        return False

    def __hash__(self):
        return hash(self.page_id)

    def __repr__(self):
        return f"PageId({self.page_id})"

class DiskManager:
    def __init__(self, heap_file: str):
        self.heap_file = heap_file
        self.file = open(heap_file, 'r+b')
        self.file.seek(0, os.SEEK_END)
        self.next_page_id = self.file.tell() // PAGE_SIZE

    @staticmethod
    def open(heap_file_path: str) -> 'DiskManager':
        if not os.path.exists(heap_file_path):
            with open(heap_file_path, 'w+b') as f:
                pass
        return DiskManager(heap_file_path)

    def read_page_data(self, page_id: PageId, data: bytearray) -> None:
        offset = PAGE_SIZE * page_id.to_u64()
        self.file.seek(offset)
        self.file.readinto(data)

    def write_page_data(self, page_id: PageId, data: bytes) -> None:
        offset = PAGE_SIZE * page_id.to_u64()
        self.file.seek(offset)
        self.file.write(data)

    def allocate_page(self) -> PageId:
        page_id = self.next_page_id
        self.next_page_id += 1
        return PageId(page_id)

    def sync(self) -> None:
        self.file.flush()
        os.fsync(self.file.fileno())

    def __del__(self):
        self.file.close()

# テスト用コード
if __name__ == "__main__":
    import tempfile

    with tempfile.NamedTemporaryFile(delete=False) as temp_file:
        temp_file_path = temp_file.name

    try:
        disk = DiskManager.open(temp_file_path)
        hello = bytearray(b"hello" + b"\x00" * (PAGE_SIZE - 5))
        world = bytearray(b"world" + b"\x00" * (PAGE_SIZE - 5))

        hello_page_id = disk.allocate_page()
        disk.write_page_data(hello_page_id, hello)

        world_page_id = disk.allocate_page()
        disk.write_page_data(world_page_id, world)

        read_hello = bytearray(PAGE_SIZE)
        disk.read_page_data(hello_page_id, read_hello)
        assert read_hello == hello

        read_world = bytearray(PAGE_SIZE)
        disk.read_page_data(world_page_id, read_world)
        assert read_world == world

        print("DiskManager tests passed.")
    finally:
        os.remove(temp_file_path)
