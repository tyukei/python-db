import sys
from buffer import BufferPool, BufferPoolManager
from disk import DiskManager, PageId
from btree import BPlusTree, SearchMode
from table import SimpleTable  # SimpleTableをインポート
import struct

def test_table(bufmgr):
    """
    テーブルの作成とレコードの挿入をテストします。
    """
    # SimpleTableのインスタンスを作成します。
    table = SimpleTable(meta_page_id=PageId(0), num_key_elems=1)
    
    # テーブルを作成します。
    table.create(bufmgr)
    print("テーブルが作成されました。")
    
    # 複数のレコードをテーブルに挿入します。
    records = [
        [b"z", b"Alice", b"Smith"],
        [b"x", b"Bob", b"Johnson"],
        [b"y", b"Charlie", b"Williams"],
        [b"w", b"Dave", b"Miller"],
        [b"v", b"Eve", b"Brown"]
    ]
    
    for record in records:
        table.insert(bufmgr, record)
        print(f"レコードを挿入しました: {record}")

def main():
    try:
        # ディスクマネージャを初期化
        disk = DiskManager.open("simple.rly")
        # バッファプールを作成
        pool = BufferPool(10)
        
        # バッファプールマネージャを作成
        bufmgr = BufferPoolManager(disk, pool)
        
        # SimpleTableのインスタンスを作成します。
        table = SimpleTable(meta_page_id=PageId(0), num_key_elems=1)
        
        # テーブルを作成します。
        table.create(bufmgr)
        print(table)
        
        # # 複数のレコードをテーブルに挿入します。
        # table.insert(bufmgr, [b"z", b"Alice", b"Smith"])
        # table.insert(bufmgr, [b"x", b"Bob", b"Johnson"])
        # table.insert(bufmgr, [b"y", b"Charlie", b"Williams"])
        # table.insert(bufmgr, [b"w", b"Dave", b"Miller"])
        # table.insert(bufmgr, [b"v", b"Eve", b"Brown"])
        
        # # バッファをフラッシュします。
        # bufmgr.flush()
    
    except Exception as e:
        print(f"エラーが発生しました: {e}")

if __name__ == "__main__":
    main()

