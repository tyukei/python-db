from buffer import BufferPool, BufferPoolManager
from disk import DiskManager, PageId
from btree import BPlusTree, SearchMode
import tuple
import struct

"""
tableとは: テーブルは、データベースの中でデータを格納するための構造です。
プライマリーキーをキーとし、その他のデータを値としてb+treeに格納します。
なぜ使うか:

"""

# SimpleTableクラスは、簡単なテーブルの管理を行います。
# このクラスは、テーブルの作成、レコードの挿入を行います。
class SimpleTable:
    def __init__(self, meta_page_id: PageId = None, num_key_elems: int = 0):
        """
        SimpleTableのコンストラクタ。

        :param meta_page_id: メタページのID
        :param num_key_elems: キーとして使用する要素数
        """
        self.meta_page_id = meta_page_id
        self.num_key_elems = num_key_elems
        self.btree = None  # 後で B+Tree インスタンスを格納

    def create(self, bufmgr: BufferPoolManager) -> None:
        """
        テーブル作成。B+Tree を初期化して self.btree に保持する。
        """
        self.btree = BPlusTree.create(bufmgr)
        self.meta_page_id = self.btree.meta_page_id

    def insert(self, bufmgr: BufferPoolManager, record: list[bytes]) -> None:
        """
        レコードをテーブルに挿入する。

        :param record: キー部と値部のバイト列リスト（例: [b"z", b"Alice", b"Smith"]）
        """
        btree = self.btree
        key = []
        tuple.encode(record[:self.num_key_elems], key)
        value = []
        tuple.encode(record[self.num_key_elems:], value)
        if not all(isinstance(item, bytes) for item in record):
            raise ValueError("All elements in the record must be of type bytes.")
        key = bytes(key)
        value = bytes(value)
        btree.insert(bufmgr=bufmgr, key=key, value=value)
        
    def show(self, bufmgr: BufferPoolManager):
        try:
            btree = self.btree
            # 全件走査のため、全てのキーが含まれる範囲を指定（ここでは空文字～大きな値）
            results = btree.search_range(bufmgr, b'', b'\xff' * 16)
            if not results:
                print("No records found.")
            else:
                for key, value in results:
                    record = []
                    tuple.decode(key, record)
                    tuple.decode(value, record)
                    print(tuple.Pretty(record))
            return True
        except Exception as e:
            print(f"Error: {e}")

class UniqueIndex:
    def __init__(self, meta_page_id: PageId = None, skey: list[int] = None):
        """
        :param meta_page_id: メタページのID
        :param skey: インデックスとして使用するフィールドのインデックスリスト
        """
        self.meta_page_id = meta_page_id
        self.skey = skey or []
        self.btree = None

    def create(self, bufmgr: BufferPoolManager) -> None:
        """
        一意のインデックス作成。B+Tree を生成して self.btree に保持する。
        """
        self.btree = BPlusTree.create(bufmgr)
        self.meta_page_id = self.btree.meta_page_id

    def insert(self, bufmgr: BufferPoolManager, pkey: bytes, record: list[bytes]) -> None:
        """
        一意インデックスにレコードを挿入する。
        :param bufmgr: バッファプールマネージャーのインスタンス
        :param pkey: プライマリキー
        :param record: 挿入するレコードのリスト
        """
        btree = self.btree
        skey = []
        # skey に該当するフィールドのみをエンコード
        tuple.encode([record[index] for index in self.skey], skey)
        btree.insert(bufmgr, bytes(skey), pkey)
    
    def show(self, bufmgr: BufferPoolManager):
        try:
            btree = self.btree
            results = btree.search_range(bufmgr, b'', b'\xff' * 16)
            if not results:
                print("No records found.")
            else:
                for key, value in results:
                    record = []
                    tuple.decode(key, record)
                    tuple.decode(value, record)
                    print(tuple.Pretty(record))
            return True
        except Exception as e:
            print(f"Error: {e}")

class Table:
    def __init__(self, meta_page_id: PageId = None, num_key_elems: int = 0, unique_indices: list[UniqueIndex] = None):
        """
        :param meta_page_id: メタページのID
        :param num_key_elems: キーとして使用する要素数
        :param unique_indices: 一意インデックスのリスト
        """
        self.meta_page_id = meta_page_id
        self.num_key_elems = num_key_elems
        self.unique_indices = unique_indices or []
        self.btree = None

    def create(self, bufmgr: BufferPoolManager) -> None:
        """
        テーブルと付随する一意インデックスを作成する。
        """
        btree = BPlusTree.create(bufmgr)
        self.btree = btree
        self.meta_page_id = btree.meta_page_id
        for unique_index in self.unique_indices:
            unique_index.create(bufmgr)

    def show(self, bufmgr: BufferPoolManager):
        try:
            btree = self.btree
            results = btree.search_range(bufmgr, b'', b'\xff' * 16)
            if not results:
                print("No records found in the table.")
            else:
                for key, value in results:
                    record = []
                    tuple.decode(key, record)
                    tuple.decode(value, record)
                    print(tuple.Pretty(record))
            return True
        except Exception as e:
            print(f"Error: {e}")

    def insert(self, bufmgr: BufferPoolManager, record: list[bytes]) -> None:
        """
        テーブルとその一意インデックスにレコードを挿入する。
        """
        btree = self.btree
        key = []
        tuple.encode(record[:self.num_key_elems], key)
        value = []
        tuple.encode(record[self.num_key_elems:], value)
        btree.insert(bufmgr, bytes(key), bytes(value))
        for unique_index in self.unique_indices:
            unique_index.insert(bufmgr, bytes(key), record)

def main() -> None:
    try:
        # ディスク、バッファプール、バッファプールマネージャの初期化
        disk = DiskManager.open("simple.rly")
        pool = BufferPool(10)
        bufmgr = BufferPoolManager(disk, pool)
        
        # SimpleTable の作成
        table = SimpleTable(meta_page_id=PageId(0), num_key_elems=1)
        table.create(bufmgr)
        print(table)
        
        # 複数のレコードを挿入
        table.insert(bufmgr, [b"z", b"Alice", b"Smith"])
        table.insert(bufmgr, [b"x", b"Bob", b"Johnson"])
        table.insert(bufmgr, [b"y", b"Charlie", b"Williams"])
        table.insert(bufmgr, [b"w", b"Dave", b"Miller"])
        table.insert(bufmgr, [b"v", b"Eve", b"Brown"])
        
        # バッファフラッシュ
        bufmgr.flush()
        print("test")
        table.show(bufmgr)
    
    except Exception as e:
        print(f"エラーが発生しました: {e}")

if __name__ == "__main__":
    main()
