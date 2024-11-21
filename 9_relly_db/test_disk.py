import os
import tempfile
from disk import DiskManager, PageId, PAGE_SIZE

def test_disk_manager():
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

    finally:
        os.remove(temp_file_path)
