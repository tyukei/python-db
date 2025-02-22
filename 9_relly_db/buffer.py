import os
import struct
from collections import defaultdict
from typing import Dict, Optional, Tuple
from disk import DiskManager, PageId, PAGE_SIZE


"""
バッファプールとは: ページのデータ(読み書きするデータ)を保持するメモリ領域
バッファプールの目的: ディスクアクセスを効率化するために、ディスクから読み込んだページをメモリ上に保持しておく
なぜ: ディスクアクセスはメモリアクセスよりも遅いため、ディスクアクセスを減らすことでパフォーマンス向上が期待できる
ファイルシステムのキャッシュ機能を使えばいい？: それでも良い、しかしファイルシステムのキャッシュを無効にしRDBMSの独自のタイミングを使用する方が賢い場合もあるため、多くのRDBMSは独自のバッファプールを持っている
(わからないこと: DiskMagerでは、このファイルシステムのブロックサイズが4kbだったことで、ページサイズも4kbにしていたが、もしファイルシステムを使わないのなら、ブロックサイズを4kbにしなくてもいいのでは？)
どうやってメモリ上に保持するか: バッファは複数あり、ディスクマネージャから読み込んだページをバッファに格納する。どのページがどのバッファに格納されているかは、PageIdとBufferIdのマッピングテーブルしたページテーブルで管理する
"""


# バッファ関連の例外クラス
class BufferError(Exception):
    """バッファプール関連で起こる一般的なエラーの基底クラス"""
    pass


class NoFreeBufferError(BufferError):
    """バッファプール内に空きフレームが無い場合に送出される例外"""
    pass


# バッファプール内部で使用するID関連クラス
class BufferId:
    """
    バッファプールの中のフレームを識別するID。
    例えば buffer_id=0 は「0番目のフレーム」を表す。
    """
    def __init__(self, buffer_id: int): # バッファIDを初期化
        self.buffer_id = buffer_id

    def __eq__(self, other): # バッファIDが等しいかどうかを比較.
        if isinstance(other, BufferId):
            return self.buffer_id == other.buffer_id
        return False

    def __hash__(self): # ハッシュ値を返す
        return hash(self.buffer_id)

    def __repr__(self): # デバッグ用に文字列表現を返す
        return f"BufferId({self.buffer_id})"


class Buffer:
    """
    1ページ分のデータを保持するクラス。
    page_id: ディスク上のどのページに対応しているか
    page: 実際のページデータ（バイナリ配列）
    is_dirty: 変更済みかどうかのフラグ
    """
    def __init__(self, page_id: PageId):
        self.page_id = page_id               # ディスク上のページID
        self.page = bytearray(PAGE_SIZE)     # ページサイズ分のバッファ領域確保。ここにデータを読み書きする
        self.is_dirty = False                # 変更があった場合 True


class Frame:
    """
    Buffer をラップし、使用回数(usage_count)などの
    バッファ置換アルゴリズムに必要な情報を保持するクラス。
    """
    def __init__(self, buffer: Buffer):
        self.usage_count = 0   # バッファ置換アルゴリズム用の使用頻度カウンタ
        self.buffer = buffer   # 実際のページデータを保持する Buffer オブジェクト


# バッファプール (フレーム配列) を管理するクラス=> 複数のフレームを管理する => 複数のバッファを管理する => 複数のページを管理する
class BufferPool:
    """
    バッファプールの実体。pool_size 個の Frame を用意し、リストで保持する。
    また、next_victim_id を持ち、バッファ置換の際に次に探すフレームIDを追跡する。
    """
    def __init__(self, pool_size: int):
        # INVALID_PAGE_IDを持つ Buffer をフレームに詰めて pool_size 個用意
        self.buffers = [Frame(Buffer(PageId(PageId.INVALID_PAGE_ID))) for _ in range(pool_size)]
        # バッファ置換アルゴリズムで最初に探しに行くフレームID
        self.next_victim_id = BufferId(0)

    def size(self) -> int:
        """バッファプールのフレーム数を返す。"""
        return len(self.buffers)

    def evict(self) -> Optional[BufferId]: # Clock-sweep(PostgresSQLにも採用されているアルゴリズム)を利用して、捨てるBuffer IDを返す
        """
        バッファプール内のすべてのフレームを調べ、捨てるバッファを見つける。
        置換対象 (victim) のフレームを探す。
        usage_count == 0 のフレームがあればそれを返し、
        なければ is_dirty のフレームの usage_count を減らしながら再試行する。
        全フレーム連続で見つからなければ None を返す（置換不可を意味する）。
        """
        pool_size = self.size()
        consecutive_pinned = 0  # usage_count > 0 のフレームが連続何個出たか

        while True: # バッファプール内のすべてのフレームを調べ、捨てるバッファを見つけるための巡回
            frame = self.buffers[self.next_victim_id.buffer_id]

            # usage_count == 0 の場合はこのフレームを返して置換に使う
            if frame.usage_count == 0:
                return self.next_victim_id

            # usage_countが非0 かつ is_dirty の場合、ディスクへの書き戻しが必要なため
            # usage_countを少し減らして再度チャンスを与える (簡易的なアルゴリズム)
            if frame.buffer.is_dirty:
                frame.usage_count -= 1
                consecutive_pinned = 0
            else:
                # is_dirty でないのに usage_count > 0 のフレームがある
                consecutive_pinned += 1
                # もしプール全体を一周しても空きが無い場合は None を返す
                if consecutive_pinned >= pool_size:
                    return None

            # 次のフレームIDへローテーション
            self.next_victim_id = BufferId((self.next_victim_id.buffer_id + 1) % pool_size)


# ディスクとバッファプールを連携させるマネージャクラス
class BufferPoolManager:
    """
    ディスクからページを読み込んだり書き出したりする際に、
    バッファプールを介して管理を行うクラス。
    page_table は、PageId -> BufferId のマッピングテーブルで、
    どのディスクページがバッファプールのどのフレームに入っているかを管理する。
    """
    def __init__(self, disk: DiskManager, pool: BufferPool):
        self.disk = disk               # ディスクマネージャ
        self.pool = pool               # バッファプール
        self.page_table: Dict[PageId, BufferId] = {}  # ページIDとバッファIDのマッピング

    def fetch_page(self, page_id: PageId) -> Buffer:
        """
        指定した page_id のページデータをメモリ上に確保し、Buffer を返す。
        もし既に読み込まれている場合は usage_count を上げて再利用。
        まだなら evict() でフレームを確保し、ディスクから読み込む。
        """
        # すでに page_table に存在する場合は再利用
        if page_id in self.page_table:
            buffer_id = self.page_table[page_id]
            frame = self.pool.buffers[buffer_id.buffer_id]
            frame.usage_count += 1  # 使用頻度を上げる
            return frame.buffer

        # ページがまだロードされていない場合
        buffer_id = self.pool.evict()
        if buffer_id is None:
            # evict() が None を返したら空きフレームなし
            raise NoFreeBufferError("No free buffer available in buffer pool")

        frame = self.pool.buffers[buffer_id.buffer_id]
        evict_page_id = frame.buffer.page_id

        # 現在のフレームに古いデータがあり、かつ is_dirty ならディスクへ書き戻す
        if frame.buffer.is_dirty:
            self.disk.write_page_data(evict_page_id, frame.buffer.page)

        # 新しいページIDを割り当てて、ディスクから読み込む
        frame.buffer.page_id = page_id
        frame.buffer.is_dirty = False
        self.disk.read_page_data(page_id, frame.buffer.page)
        frame.usage_count = 1

        # page_table のエントリを更新
        self.page_table.pop(evict_page_id, None)  # 古いページIDを削除
        self.page_table[page_id] = buffer_id

        return frame.buffer

    def create_page(self) -> Buffer:
        """
        新たにページをディスクに確保して、それをバッファプールに載せる。
        返り値は作成したページの Buffer オブジェクト。
        """
        # 空きフレームを確保
        buffer_id = self.pool.evict()
        if buffer_id is None:
            raise NoFreeBufferError("No free buffer available in buffer pool")

        frame = self.pool.buffers[buffer_id.buffer_id]
        evict_page_id = frame.buffer.page_id

        # 古いフレームが is_dirty なら書き戻し
        if frame.buffer.is_dirty:
            self.disk.write_page_data(evict_page_id, frame.buffer.page)

        # ディスク上で新たにページIDを割り当て
        page_id = self.disk.allocate_page()

        # フレームに新しいBufferをはめこむ
        frame.buffer = Buffer(page_id)
        frame.buffer.is_dirty = True   # まだ中身を初期化していないので変更あり扱い
        frame.usage_count = 1

        # page_table のエントリを更新
        self.page_table.pop(evict_page_id, None)
        self.page_table[page_id] = buffer_id

        return frame.buffer

    def flush(self) -> None:
        """
        バッファプール上の全ての dirty ページをディスクに書き込む。
        最後に disk.sync() を呼んで、物理ディスクへの同期を保証する。
        """
        print("Flushing buffers to disk...")
        for page_id, buffer_id in self.page_table.items():
            frame = self.pool.buffers[buffer_id.buffer_id]
            # 変更フラグが立っている場合はディスクへ書き戻し
            if frame.buffer.is_dirty:
                print(f"Flushing page {page_id.page_id} to disk")
                self.disk.write_page_data(page_id, frame.buffer.page)
                frame.buffer.is_dirty = False

        # 書き込みの完了をOSに確定させる
        self.disk.sync()


#------------------------------------------------------------------------------
# テスト用コード
#------------------------------------------------------------------------------
if __name__ == "__main__":
    import tempfile

    # 一時ファイルを作ってテストを行い、最後に削除する
    with tempfile.NamedTemporaryFile(delete=False) as temp_file:
        temp_file_path = temp_file.name

    try:
        # ディスクマネージャとバッファプールの準備
        disk = DiskManager.open(temp_file_path)
        pool = BufferPool(1000)  # バッファプールのサイズは自由に設定
        bufmgr = BufferPoolManager(disk, pool)

        # テスト用データ作成
        hello = bytearray(b"hello" + b"\x00" * (PAGE_SIZE - 5))
        world = bytearray(b"world" + b"\x00" * (PAGE_SIZE - 5))

        # 新規ページを2つ作成し、それぞれにデータ書き込み
        buffer1 = bufmgr.create_page()
        buffer1.page[:5] = b"hello"
        buffer1.is_dirty = True
        page1_id = buffer1.page_id

        buffer2 = bufmgr.create_page()
        buffer2.page[:5] = b"world"
        buffer2.is_dirty = True
        page2_id = buffer2.page_id

        # ディスクにフラッシュ
        bufmgr.flush()

        # 再度ページを読み込みして内容確認
        buffer1_read = bufmgr.fetch_page(page1_id)
        assert buffer1_read.page[:5] == b"hello"

        buffer2_read = bufmgr.fetch_page(page2_id)
        assert buffer2_read.page[:5] == b"world"

        print("BufferPoolManager tests passed.")
    finally:
        # テスト終了後にファイル削除
        os.remove(temp_file_path)