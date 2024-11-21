import os
import tempfile
from buffer import BufferPool, BufferPoolManager, NoFreeBufferError
from disk import DiskManager, PAGE_SIZE

def test_buffer_pool_manager():
    with tempfile.NamedTemporaryFile(delete=False) as temp_file:
        temp_file_path = temp_file.name

    try:
        disk = DiskManager.open(temp_file_path)
        pool = BufferPool(2)  # バッファプールのサイズを2に増やす
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

    finally:
        os.remove(temp_file_path)

if __name__ == "__main__":
    test_buffer_pool_manager()
