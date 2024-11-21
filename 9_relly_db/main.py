import sys
from buffer import BufferPool, BufferPoolManager
from disk import DiskManager, PageId
from btree import BTree, SearchMode
import struct

def main():
    try:
        # ディスクマネージャを初期化
        disk = DiskManager.open("simple.rly")
        
        # バッファプールを作成
        pool = BufferPool(10)
        
        # バッファプールマネージャを作成
        bufmgr = BufferPoolManager(disk, pool)
        
        # BTreeを作成
        btree = BTree.create(bufmgr)
        
        # データを挿入
        print("Inserting data...")
        btree.insert(bufmgr, struct.pack('>Q', 6), b"world")
        btree.insert(bufmgr, struct.pack('>Q', 3), b"hello")
        btree.insert(bufmgr, struct.pack('>Q', 8), b"!")
        btree.insert(bufmgr, struct.pack('>Q', 4), b",")
        
        # データをフラッシュ
        bufmgr.flush()
        
        # データを検索
        print("Searching data...")
        iter = btree.search(bufmgr, SearchMode.Key(struct.pack('>Q', 3)))
        key, value = iter.get()
        print(f"Key: {struct.unpack('>Q', key)[0]}, Value: {value.decode()}")
        
        iter = btree.search(bufmgr, SearchMode.Key(struct.pack('>Q', 8)))
        key, value = iter.get()
        print(f"Key: {struct.unpack('>Q', key)[0]}, Value: {value.decode()}")
        
    except Exception as e:
        print(f"An error occurred: {e}", file=sys.stderr)

if __name__ == "__main__":
    main()
