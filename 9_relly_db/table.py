from buffer import BufferPool, BufferPoolManager
from disk import DiskManager, PageId
from btree import BPlusTree, SearchMode
import tuple

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
        :param num_key_elems: キーとして使用する要素の数
        """
        self.meta_page_id = meta_page_id
        self.num_key_elems = num_key_elems

    def create(self, bufmgr: BufferPoolManager) -> None:
        """
        テーブルを作成し、BtreePlusを初期化します。
        
        :param bufmgr: バッファプールマネージャーのインスタンス
        """
        btree = BPlusTree.create(bufmgr)
        self.meta_page_id = btree.meta_page_id

    def insert(self, bufmgr: BufferPoolManager, record: list[list[bytes]]) -> None:
        """
        レコードをテーブルに挿入します。
        
        :param bufmgr: バッファプールマネージャーのインスタンス
        :param record: 挿入するレコードのリスト
        """
        btree = BPlusTree(self.meta_page_id)
        key = []
        tuple.encode(record[:self.num_key_elems], key)
        value = []
        tuple.encode(record[self.num_key_elems:], value)
        if not all(isinstance(item, bytes) for item in record):
            raise ValueError("All elements in the record must be of type bytes.")
        print(f"1key type is {type(key)}:{key}")
        # change list to bytes
        key = bytes(key)
        print(f"2key type is {type(key)}:{key}")
        value = bytes(value)
        btree.insert(bufmgr=bufmgr, key=key, value=value)
        print(f"Record before encoding: {record}")
        
    def show(self,bufmgr: BufferPoolManager):
        try:            
            btree = BPlusTree(self.meta_page_id)
            print("ok1")
            iter = btree.search(bufmgr, SearchMode.START)
            print("ok2")

            while True:
                result = iter.next(bufmgr)
                if result is None:
                    break

                key, value = result
                record = []
                tuple.decode(key, record)
                tuple.decode(value, record)
                print(tuple.Pretty(record))
            return True
            
        except Exception as e:
            print(f"Error: {e}")


        

# UniqueIndexクラスは、一意のインデックスを管理します。
class UniqueIndex:
    def __init__(self, meta_page_id: PageId = None, skey: list[int] = None):
        """
        UniqueIndexのコンストラクタ。
        
        :param meta_page_id: メタページのID
        :param skey: インデックスとして使用するキーのインデックスリスト
        """
        self.meta_page_id = meta_page_id
        self.skey = skey or []

    def create(self, bufmgr: BufferPoolManager) -> None:
        """
        一意のインデックスを作成し、BtreePlusを初期化します。
        
        :param bufmgr: バッファプールマネージャーのインスタンス
        """
        btree = BPlusTree.create(bufmgr)
        self.meta_page_id = btree.meta_page_id

    def insert(self, bufmgr: BufferPoolManager, pkey: bytes, record: list[bytes]) -> None:
        """
        一意のインデックスにレコードを挿入します。
        
        :param bufmgr: バッファプールマネージャーのインスタンス
        :param pkey: プライマリキー
        :param record: 挿入するレコードのリスト
        """
        btree = BPlusTree(self.meta_page_id)
        skey = []
        tuple.encode([record[index] for index in self.skey], skey)
        btree.insert(bufmgr, skey, pkey)
    
    def show(self,bufmgr: BufferPoolManager):
        try:            
            btree = BPlusTree(self.meta_page_id)
            iter = btree.search(bufmgr, SearchMode.START)
            while True:
                result = iter.next(bufmgr)
                if result is None:
                    break
                    
                key, value = result
                record = []
                tuple.decode(key, record)
                tuple.decode(value, record)
                print(tuple.Pretty(record))
            return True
            
        except Exception as e:
            print(f"Error: {e}")



# Tableクラスは、複数のUniqueIndexを持つテーブルを管理します。
class Table:
    def __init__(self, meta_page_id: PageId = None, num_key_elems: int = 0, unique_indices: list[UniqueIndex] = None):
        """
        Tableのコンストラクタ。
        
        :param meta_page_id: メタページのID
        :param num_key_elems: キーとして使用する要素の数
        :param unique_indices: 一意のインデックスのリスト
        """
        self.meta_page_id = meta_page_id
        self.num_key_elems = num_key_elems
        self.unique_indices = unique_indices or []

    def create(self, bufmgr: BufferPoolManager) -> None:
        """
        テーブルとその一意のインデックスを作成します。
        
        :param bufmgr: バッファプールマネージャーのインスタンス
        """
        btree = BPlusTree.create(bufmgr)
        self.meta_page_id = btree.meta_page_id
        for unique_index in self.unique_indices:
            unique_index.create(bufmgr)

    def insert(self, bufmgr: BufferPoolManager, record: list[list[bytes]]) -> None:
        """
        レコードをテーブルとその一意のインデックスに挿入します。
        
        :param bufmgr: バッファプールマネージャーのインスタンス
        :param record: 挿入するレコードのリスト
        """
        btree = BPlusTree(self.meta_page_id)
        key = []
        tuple.encode(record[:self.num_key_elems], key)
        value = []
        tuple.encode(record[self.num_key_elems:], value)
        btree.insert(bufmgr, bytes(key), bytes(value))
        for unique_index in self.unique_indices:
            unique_index.insert(bufmgr, key, record)

# 以下にテスト関数を追加します。

def main() -> None:
    """
    テーブルの作成とレコードの挿入をテストします。
    """
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
        
        # 複数のレコードをテーブルに挿入します。
        table.insert(bufmgr, [b"z", b"Alice", b"Smith"])
        table.insert(bufmgr, [b"x", b"Bob", b"Johnson"])
        table.insert(bufmgr, [b"y", b"Charlie", b"Williams"])
        table.insert(bufmgr, [b"w", b"Dave", b"Miller"])
        table.insert(bufmgr, [b"v", b"Eve", b"Brown"])
        
        # バッファをフラッシュします。

        bufmgr.flush()
        print("test")
        table.show(bufmgr)
    
    except Exception as e:
        print(f"エラーが発生しました: {e}")

if __name__ == "__main__":
    main()
