import sys
from buffer import BufferPool, BufferPoolManager
from disk import DiskManager, PageId
from btree import BPlusTree, SearchMode
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
        btree = BPlusTree.create(bufmgr)
        
        # データを挿入
        print("Inserting data...")
        btree.insert(bufmgr, struct.pack('>Q', 1), b"one")
        btree.insert(bufmgr, struct.pack('>Q', 4), b"two")
        btree.insert(bufmgr, struct.pack('>Q', 6), b"three")
        btree.insert(bufmgr, struct.pack('>Q', 3), b"four")
        btree.insert(bufmgr, struct.pack('>Q', 7), b"five")
        btree.insert(bufmgr, struct.pack('>Q', 2), b"six")
        btree.insert(bufmgr, struct.pack('>Q', 5), b"seven")                       
        # データをフラッシュ
        bufmgr.flush()
        
        # データを検索
        print("Searching data...")
        result = btree.search(bufmgr, SearchMode.Key(struct.pack('>Q', 3)))
        if result:  # 検索結果がある場合のみ処理
            key, value = result  # タプルをアンパック
            print(f"Key: {struct.unpack('>Q', key)[0]}, Value: {value.decode()}")
        else:
            print("Key not found.")

        result = btree.search(bufmgr, SearchMode.Key(struct.pack('>Q', 8)))
        if result:  # 検索結果がある場合のみ処理
            key, value = result  # タプルをアンパック
            print(f"Key: {struct.unpack('>Q', key)[0]}, Value: {value.decode()}")
        else:
            print("Key not found.")
        
    except Exception as e:
        print(f"An error occurred: {e}", file=sys.stderr)

if __name__ == "__main__":
    main()
