import os
import struct
from typing import Optional

"""
DiskManagerとは: ディスク上のページを管理するクラス
DiskManagerの目的: データをディスクに永続化する
なぜDiskManagerを使うか: アプリを終了しても、データを保持したい
どうやって: ヒープファイルを使ってデータを永続化する
ヒープファイルとは: データを自由に読み書きできるファイル
なぜヒープファイルを使う: ファイルに読み書きする時、ファイルシステムがHDD, SSDにアクセスするが、ブロックという単位(4kbが多い)でアクセスする。
ファイルシステムが勝手にブロックサイズに切り上げてしまうため、データが小さい場合、無駄が生じる。
(分からないこと:逆に、データが大きい場合、ブロックサイズに収まらない場合、複数のブロックに分割され、データの整合性が取れなくなる可能性がある?)
どうやってヒープファイルを使う: ページIDを使ってデータを読み書きする。ポインタ、配列みたいなイメージ
ページとは: データを読み書きする単位。4096バイトの正数倍(1~4倍)が多い。
なぜ4kBのページサイズ: Linuxで使われるファイルシステム(ext4)のデフォルトのブロックサイズが4kBだから。
"""

PAGE_SIZE = 4096

class PageId:
    # 64ビットの符号なし整数（uint64_t）の最大値。
    INVALID_PAGE_ID = 2**64 - 1

    def __init__(self, page_id: int):
        self.page_id = page_id

    # # ページIDが有効かどうか
    # def valid(self) -> Optional['PageId']:
    #     if self.page_id == PageId.INVALID_PAGE_ID:
    #         return None
    #     return self

    # ページIDを64ビットの整数として返す
    def to_u64(self) -> int:
        return self.page_id
    
    # バイト列からPageIdオブジェクトを生成する
    @staticmethod
    def from_bytes(bytes_data: bytes) -> 'PageId':
        page_id, = struct.unpack('Q', bytes_data)
        return PageId(page_id)
    
    # PageIdオブジェクトをバイト列に変換
    def to_bytes(self) -> bytes:
        return struct.pack('Q', self.page_id)
    
     # PageIdオブジェクトの等しいかどうかを比較
    def __eq__(self, other):
        if isinstance(other, PageId):
            return self.page_id == other.page_id
        return False
    
    # PageIdオブジェクトのハッシュ値を返す
    def __hash__(self):
        return hash(self.page_id)
    
    # PageIdオブジェクトの文字列表現を返す
    def __repr__(self):
        return f"PageId({self.page_id})"

class DiskManager:
    # ヒープファイル(自由に読み書きできるファイル)を指定してDiskManagerを作成
    # 次に使用するページIDを設定
    def __init__(self, heap_file: str): 
        self.heap_file = heap_file
        self.file = open(heap_file, 'r+b')
        self.file.seek(0, os.SEEK_END) #　ファイルの0バイト目からファイルの最後まで移動
        self.next_page_id = self.file.tell() // PAGE_SIZE #　現在の位置(最後)をページサイズで割る。例えば、8192バイトの場合、8192 // 4096 = 2。ファイルが新規作成された場合、ファイルサイズは 0 バイトであり、ページ ID は 0。

    # ヒープファイルを開き、存在しない場合は新規作成
    @staticmethod
    def open(heap_file_path: str) -> 'DiskManager':
        if not os.path.exists(heap_file_path):
            with open(heap_file_path, 'w+b') as f:
                pass
        return DiskManager(heap_file_path)
    
    # 指定したページIDのデータを読み込む
    def read_page_data(self, page_id: PageId, data: bytearray) -> None:
        offset = PAGE_SIZE * page_id.to_u64()
        self.file.seek(offset)
        self.file.readinto(data)

    # 指定したページIDにデータを書き込む
    def write_page_data(self, page_id: PageId, data: bytes) -> None:
        offset = PAGE_SIZE * page_id.to_u64()
        self.file.seek(offset)
        self.file.write(data)

    # ページを割り当て、ページIDを返す
    def allocate_page(self) -> PageId:
        page_id = self.next_page_id
        self.next_page_id += 1
        return PageId(page_id)
    
    # データをディスクに書き込む(永続化)
    def sync(self) -> None:
        self.file.flush()
        os.fsync(self.file.fileno())

    # ディスクマネージャを閉じる
    def __del__(self):
        self.file.close()

# テスト用コード
if __name__ == "__main__":
    import tempfile

    # 一時ファイルを作成(バッファプールの代わり)
    with tempfile.NamedTemporaryFile(delete=False) as temp_file:
        temp_file_path = temp_file.name

    try:
        # DiskManagerを開く
        disk = DiskManager.open(temp_file_path)
        
        # "hello"と"world"のデータを準備
        hello = bytearray(b"hello" + b"\x00" * (PAGE_SIZE - 5)) # b""は文字列をバイト列に変換する。b"\x00"はNULL文字
        world = bytearray(b"world" + b"\x00" * (PAGE_SIZE - 5))

        # ページを割り当ててデータを書き込む
        hello_page_id = disk.allocate_page()
        disk.write_page_data(hello_page_id, hello)

        world_page_id = disk.allocate_page()
        disk.write_page_data(world_page_id, world)

        # データを読み込んで検証
        read_hello = bytearray(PAGE_SIZE)
        disk.read_page_data(hello_page_id, read_hello)
        assert read_hello == hello

        read_world = bytearray(PAGE_SIZE)
        disk.read_page_data(world_page_id, read_world)
        assert read_world == world

        print("DiskManager tests passed.")
    finally:
        # 一時ファイルを削除
        os.remove(temp_file_path)
