import struct
from typing import Optional, Tuple, List
from buffer import BufferPoolManager, Buffer
from disk import PageId, PAGE_SIZE
import pickle
import os

# 例外クラス定義
class BTreeError(Exception):
    """B+ツリーに関連する基本的な例外クラス"""
    pass

class DuplicateKeyError(BTreeError):
    """重複するキーの挿入を試みた際に発生する例外"""
    pass

# ノードタイプ定義
class NodeType:
    LEAF = 0    # リーフノード
    BRANCH = 1  # ブランチノード（内部ノード）

# 検索モード定義クラス
class SearchMode:
    START = 0  # 開始位置から検索
    KEY = 1    # 特定のキーで検索

    def __init__(self, mode: int, key: Optional[bytes] = None):
        """
        検索モードの初期化

        Args:
            mode (int): 検索モード（STARTまたはKEY）
            key (Optional[bytes]): 検索対象のキー（KEYモードの場合）
        """
        self.mode = mode
        self.key = key

    @staticmethod
    def Start():
        """開始位置からの検索モードを返す"""
        return SearchMode(SearchMode.START)

    @staticmethod
    def Key(key: bytes):
        """特定のキーでの検索モードを返す"""
        return SearchMode(SearchMode.KEY, key)

# キーと値のペア管理クラス
class Pair:
    def __init__(self, key: bytes, value: bytes):
        """
        キーと値のペアを初期化

        Args:
            key (bytes): キー
            value (bytes): 値
        """
        self.key = key
        self.value = value

    def to_bytes(self) -> bytes:
        """
        Pairオブジェクトをバイト列にシリアライズ

        Returns:
            bytes: シリアライズされたバイト列
        """
        return pickle.dumps(self)

    @staticmethod
    def from_bytes(data: bytes) -> 'Pair':
        """
        バイト列からPairオブジェクトをデシリアライズ

        Args:
            data (bytes): シリアライズされたバイト列

        Returns:
            Pair: デシリアライズされたPairオブジェクト
        """
        return pickle.loads(data)

# B+Treeクラス
class BPlusTree:
    LEAF_NODE_MAX_PAIRS = 2    # リーフノードの最大ペア数
    BRANCH_NODE_MAX_KEYS = 2   # ブランチノードの最大キー数

    def __init__(self, meta_page_id: PageId):
        """
        B+ツリーの初期化

        Args:
            meta_page_id (PageId): メタデータページのページID
        """
        self.meta_page_id = meta_page_id  # メタデータページIDの保存

    @staticmethod
    def create(bufmgr: BufferPoolManager) -> 'BPlusTree':
        """
        新しいB+ツリーを作成し、初期化する

        Args:
            bufmgr (BufferPoolManager): バッファプールマネージャ

        Returns:
            BPlusTree: 作成されたB+ツリーのインスタンス
        """
        # メタデータとルートノードの作成
        meta_buffer = bufmgr.create_page()  # メタデータページを新規作成
        root_buffer = bufmgr.create_page()  # ルートノードページを新規作成

        # ルートノードをリーフノードとして初期化
        root_buffer.page[:4] = struct.pack('>I', NodeType.LEAF)  # ノードタイプをリーフに設定
        root_buffer.page[4:8] = struct.pack('>I', 0)  # ペア数を0に初期化

        # メタデータページにルートノードのページIDを保存
        meta_buffer.page[:8] = root_buffer.page_id.to_bytes()

        # バッファのダーティフラグを設定（変更があったことを示す）
        meta_buffer.is_dirty = True
        root_buffer.is_dirty = True

        # 新しいB+ツリーのインスタンスを返す
        return BPlusTree(meta_page_id=meta_buffer.page_id)

    def fetch_root_page(self, bufmgr: BufferPoolManager) -> Buffer:
        """
        ルートページを取得する

        Args:
            bufmgr (BufferPoolManager): バッファプールマネージャ

        Returns:
            Buffer: ルートページのバッファ
        """
        meta_buffer = bufmgr.fetch_page(self.meta_page_id)  # メタデータページを取得
        root_page_id = PageId.from_bytes(meta_buffer.page[:8])  # メタデータからルートページIDを読み取る
        return bufmgr.fetch_page(root_page_id)  # ルートページのバッファを返す

    def search(self, bufmgr: BufferPoolManager, search_mode: SearchMode) -> Optional[Tuple[bytes, bytes]]:
        """
        B+ツリー内で指定された検索モードに基づいて検索を行う

        Args:
            bufmgr (BufferPoolManager): バッファプールマネージャ
            search_mode (SearchMode): 検索モード

        Returns:
            Optional[Tuple[bytes, bytes]]: 見つかったキーと値のタプル、見つからなければNone
        """
        root_page = self.fetch_root_page(bufmgr)  # ルートページを取得
        return self.search_internal(bufmgr, root_page, search_mode)  # 内部検索メソッドを呼び出す

    def search_internal(self, bufmgr: BufferPoolManager, node_buffer: Buffer, search_mode: SearchMode) -> Optional[Tuple[bytes, bytes]]:
        """
        再帰的にB+ツリーを探索し、指定されたキーを検索する

        Args:
            bufmgr (BufferPoolManager): バッファプールマネージャ
            node_buffer (Buffer): 現在探索中のノードのバッファ
            search_mode (SearchMode): 検索モード

        Returns:
            Optional[Tuple[bytes, bytes]]: 見つかったキーと値のタプル、見つからなければNone
        """
        # ノードタイプを読み取る（リーフノード=0、ブランチノード=1）
        node_type = struct.unpack('>I', node_buffer.page[:4])[0]

        if node_type == NodeType.LEAF:
            # リーフノードの場合、ペアを取得してキーを検索
            pairs = self.get_pairs(node_buffer)
            for pair in pairs:
                if search_mode.key and pair.key == search_mode.key:
                    return pair.key, pair.value  # キーが一致した場合、キーと値を返す
            return None  # 見つからなかった場合
        else:
            # ブランチノードの場合、キーに基づいて適切な子ノードを選択
            keys, children = self.get_branch(node_buffer)
            for i, key in enumerate(keys):
                if search_mode.key < key:
                    # 指定されたキーが現在のキーより小さい場合、対応する子ノードに進む
                    child_page_id = children[i]
                    child_buffer = bufmgr.fetch_page(child_page_id)
                    return self.search_internal(bufmgr, child_buffer, search_mode)
            # 全てのキーよりも大きい場合、最後の子ノードに進む
            child_page_id = children[-1]
            child_buffer = bufmgr.fetch_page(child_page_id)
            return self.search_internal(bufmgr, child_buffer, search_mode)

    def insert(self, bufmgr: BufferPoolManager, key: bytes, value: bytes) -> None:
        """
        B+ツリーにキーと値のペアを挿入する

        Args:
            bufmgr (BufferPoolManager): バッファプールマネージャ
            key (bytes): 挿入するキー
            value (bytes): 挿入する値
        """
        root_page = self.fetch_root_page(bufmgr)  # ルートページを取得
        new_child = self.insert_internal(bufmgr, root_page, key, value)  # 内部挿入処理を呼び出す

        if new_child is not None:
            # 挿入後、ルートノードが分割された場合、新しいルートノードを作成
            new_root_buffer = bufmgr.create_page()  # 新しいルートページを作成
            new_root_buffer.page[:4] = struct.pack('>I', NodeType.BRANCH)  # ノードタイプをブランチに設定
            new_root_buffer.is_dirty = True  # ダーティフラグを設定

            meta_buffer = bufmgr.fetch_page(self.meta_page_id)  # メタデータページを取得
            meta_buffer.page[:8] = new_root_buffer.page_id.to_bytes()  # メタデータに新しいルートページIDを設定
            meta_buffer.is_dirty = True  # ダーティフラグを設定

            new_key, new_page_id = new_child  # 分割によって昇格したキーと新しいページIDを取得
            self.set_branch(new_root_buffer, [new_key], [root_page.page_id, new_page_id])  # 新しいルートノードに設定

    def insert_internal(self, bufmgr: BufferPoolManager, node_buffer: Buffer, key: bytes, value: bytes) -> Optional[Tuple[bytes, PageId]]:
        """
        再帰的にB+ツリーにキーと値のペアを挿入し、必要に応じてノードを分割する

        Args:
            bufmgr (BufferPoolManager): バッファプールマネージャ
            node_buffer (Buffer): 現在挿入対象のノードのバッファ
            key (bytes): 挿入するキー
            value (bytes): 挿入する値

        Returns:
            Optional[Tuple[bytes, PageId]]: 分割が発生した場合、昇格したキーと新しいページIDのタプル。それ以外はNone
        """
        # ノードタイプを読み取る（リーフノード=0、ブランチノード=1）
        node_type = struct.unpack('>I', node_buffer.page[:4])[0]

        if node_type == NodeType.LEAF:
            # リーフノードの場合、ペアを取得
            pairs = self.get_pairs(node_buffer)

            # 重複キーのチェック
            for pair in pairs:
                if pair.key == key:
                    raise DuplicateKeyError("Duplicate key")

            # 新しいペアを追加
            pairs.append(Pair(key, value))
            # キーの昇順にソート
            pairs.sort(key=lambda p: p.key)

            if len(pairs) <= self.LEAF_NODE_MAX_PAIRS:
                # オーバーフローしない場合、リーフノードを更新
                self.set_leaf(node_buffer, pairs)
                node_buffer.is_dirty = True
                return None  # 分割は不要
            else:
                # リーフノードがオーバーフローした場合、分割処理を行う
                return self.split_leaf(bufmgr, node_buffer, pairs)
        else:
            # ブランチノードの場合、キーに基づいて適切な子ノードを選択
            keys, children = self.get_branch(node_buffer)
            index = 0
            # 挿入するキーがどの範囲に属するかを決定
            while index < len(keys) and key >= keys[index]:
                index += 1

            # 選択された子ノードのページIDを取得
            child_page_id = children[index]
            child_buffer = bufmgr.fetch_page(child_page_id)

            # 選択された子ノードに再帰的に挿入処理を行う
            result = self.insert_internal(bufmgr, child_buffer, key, value)

            if result is None:
                # 子ノードが分割されなかった場合、何も返さない
                return None
            else:
                # 子ノードが分割され、新しいキーとページIDが昇格された場合、親ノードに挿入
                new_key, new_page_id = result
                keys.insert(index, new_key)           # 昇格キーを親ノードのキーリストに挿入
                children.insert(index + 1, new_page_id)  # 新しい子ノードのページIDを子リストに挿入

                if len(keys) <= self.BRANCH_NODE_MAX_KEYS:
                    # ブランチノードがオーバーフローしない場合、ノードを更新
                    self.set_branch(node_buffer, keys, children)
                    node_buffer.is_dirty = True
                    return None  # 分割は不要
                else:
                    # ブランチノードがオーバーフローした場合、分割処理を行う
                    return self.split_branch(bufmgr, node_buffer, keys, children)

    def split_leaf(self, bufmgr: BufferPoolManager, node_buffer: Buffer, pairs: List[Pair]) -> Tuple[bytes, PageId]:
        """
        リーフノードを分割し、昇格させるキーと新しいリーフノードのページIDを返す

        Args:
            bufmgr (BufferPoolManager): バッファプールマネージャ
            node_buffer (Buffer): 分割対象のリーフノードのバッファ
            pairs (List[Pair]): リーフノード内のペアリスト

        Returns:
            Tuple[bytes, PageId]: 昇格させるキーと新しいリーフノードのページID
        """
        # リーフノードのペアを半分に分割
        mid = len(pairs) // 2
        left_pairs = pairs[:mid]   # 左側のペア
        right_pairs = pairs[mid:]  # 右側のペア

        # 元のリーフノードを左側のペアで更新
        self.set_leaf(node_buffer, left_pairs)
        node_buffer.is_dirty = True

        # 新しいリーフノードを作成し、右側のペアを設定
        new_leaf_buffer = bufmgr.create_page()
        new_leaf_buffer.page[:4] = struct.pack('>I', NodeType.LEAF)  # ノードタイプをリーフに設定
        self.set_leaf(new_leaf_buffer, right_pairs)
        new_leaf_buffer.is_dirty = True

        # 昇格させるキーは右側のリーフノードの最初のキー
        promote_key = right_pairs[0].key

        # 新しいリーフノードのページIDを返す
        return promote_key, new_leaf_buffer.page_id

    def split_branch(self, bufmgr: BufferPoolManager, node_buffer: Buffer, keys: List[bytes], children: List[PageId]) -> Tuple[bytes, PageId]:
        """
        ブランチノードを分割し、昇格させるキーと新しいブランチノードのページIDを返す

        Args:
            bufmgr (BufferPoolManager): バッファプールマネージャ
            node_buffer (Buffer): 分割対象のブランチノードのバッファ
            keys (List[bytes]): ブランチノード内のキーリスト
            children (List[PageId]): ブランチノード内の子ページIDリスト

        Returns:
            Tuple[bytes, PageId]: 昇格させるキーと新しいブランチノードのページID
        """
        # ブランチノードのキーを半分に分割
        mid = len(keys) // 2
        promote_key = keys[mid]  # 昇格させるキー

        left_keys = keys[:mid]         # 左側のキー
        right_keys = keys[mid + 1:]    # 右側のキー

        left_children = children[:mid + 1]      # 左側の子ページID
        right_children = children[mid + 1:]     # 右側の子ページID

        # 元のブランチノードを左側のキーと子ページIDで更新
        self.set_branch(node_buffer, left_keys, left_children)
        node_buffer.is_dirty = True

        # 新しいブランチノードを作成し、右側のキーと子ページIDを設定
        new_branch_buffer = bufmgr.create_page()
        new_branch_buffer.page[:4] = struct.pack('>I', NodeType.BRANCH)  # ノードタイプをブランチに設定
        self.set_branch(new_branch_buffer, right_keys, right_children)
        new_branch_buffer.is_dirty = True

        # 昇格させるキーと新しいブランチノードのページIDを返す
        return promote_key, new_branch_buffer.page_id

    def get_pairs(self, buffer: Buffer) -> List[Pair]:
        """
        リーフノードからペアリストを取得する

        Args:
            buffer (Buffer): リーフノードのバッファ

        Returns:
            List[Pair]: リーフノード内のペアリスト
        """
        # ペア数を読み取る（ページの4～8バイト目）
        num_pairs = struct.unpack('>I', buffer.page[4:8])[0]
        pairs = []
        offset = 8  # ペアデータの開始オフセット

        for _ in range(num_pairs):
            if offset + 4 > PAGE_SIZE:
                break  # ページの範囲外を防ぐ

            # ペアのサイズを読み取る（4バイト）
            pair_size = struct.unpack('>I', buffer.page[offset:offset+4])[0]
            # ペアのデータを読み取る
            pair_data = buffer.page[offset+4:offset+4+pair_size]
            # ペアをデシリアライズしてリストに追加
            pairs.append(Pair.from_bytes(pair_data))
            # オフセットを更新
            offset += 4 + pair_size

        return pairs

    def set_leaf(self, buffer: Buffer, pairs: List[Pair]) -> None:
        """
        リーフノードにペアリストを設定する

        Args:
            buffer (Buffer): リーフノードのバッファ
            pairs (List[Pair]): 設定するペアリスト
        """
        # ノードタイプをリーフに設定（ページの最初の4バイト）
        buffer.page[:4] = struct.pack('>I', NodeType.LEAF)
        # ペア数を設定（ページの4～8バイト目）
        buffer.page[4:8] = struct.pack('>I', len(pairs))
        offset = 8  # ペアデータの開始オフセット

        for pair in pairs:
            # ペアをバイト列にシリアライズ
            pair_data = pair.to_bytes()
            pair_size = len(pair_data)
            # ペアのサイズを設定（4バイト）
            buffer.page[offset:offset+4] = struct.pack('>I', pair_size)
            # ペアのデータを設定
            buffer.page[offset+4:offset+4+pair_size] = pair_data
            # オフセットを更新
            offset += 4 + pair_size

    def get_branch(self, buffer: Buffer) -> Tuple[List[bytes], List[PageId]]:
        """
        ブランチノードからキーリストと子ページIDリストを取得する

        Args:
            buffer (Buffer): ブランチノードのバッファ

        Returns:
            Tuple[List[bytes], List[PageId]]: キーリストと子ページIDリスト
        """
        # キー数を読み取る（ページの4～8バイト目）
        num_keys = struct.unpack('>I', buffer.page[4:8])[0]
        keys = []
        children = []
        offset = 8  # キーと子ページIDの開始オフセット

        # キーを読み取る
        for _ in range(num_keys):
            if offset + 4 > PAGE_SIZE:
                break  # ページの範囲外を防ぐ

            # キーのサイズを読み取る（4バイト）
            key_size = struct.unpack('>I', buffer.page[offset:offset+4])[0]
            # キーのデータを読み取る
            key = buffer.page[offset+4:offset+4+key_size]
            keys.append(key)
            # オフセットを更新
            offset += 4 + key_size

        # 子ページIDを読み取る（キー数 + 1 個）
        for _ in range(num_keys + 1):
            if offset + 8 > PAGE_SIZE:
                break  # ページの範囲外を防ぐ

            # 子ページIDを読み取る（8バイト）
            child_page_id = PageId.from_bytes(buffer.page[offset:offset+8])
            children.append(child_page_id)
            # オフセットを更新
            offset += 8

        return keys, children

    def set_branch(self, buffer: Buffer, keys: List[bytes], children: List[PageId]) -> None:
        """
        ブランチノードにキーリストと子ページIDリストを設定する

        Args:
            buffer (Buffer): ブランチノードのバッファ
            keys (List[bytes]): 設定するキーリスト
            children (List[PageId]): 設定する子ページIDリスト
        """
        # ノードタイプをブランチに設定（ページの最初の4バイト）
        buffer.page[:4] = struct.pack('>I', NodeType.BRANCH)
        # キー数を設定（ページの4～8バイト目）
        buffer.page[4:8] = struct.pack('>I', len(keys))
        offset = 8  # キーと子ページIDの開始オフセット

        # キーを設定
        for key in keys:
            key_size = len(key)
            if offset + 4 + key_size > PAGE_SIZE:
                break  # ページの範囲外を防ぐ

            # キーのサイズを設定（4バイト）
            buffer.page[offset:offset+4] = struct.pack('>I', key_size)
            # キーのデータを設定
            buffer.page[offset+4:offset+4+key_size] = key
            # オフセットを更新
            offset += 4 + key_size

        # 子ページIDを設定
        for child in children:
            if offset + 8 > PAGE_SIZE:
                break  # ページの範囲外を防ぐ

            # 子ページIDを設定（8バイト）
            buffer.page[offset:offset+8] = child.to_bytes()
            # オフセットを更新
            offset += 8

    def search_range(self, bufmgr: BufferPoolManager, start_key: bytes, end_key: bytes) -> List[Tuple[bytes, bytes]]:
        """
        指定された範囲内のキーと値を検索する（オプション）

        Args:
            bufmgr (BufferPoolManager): バッファプールマネージャ
            start_key (bytes): 範囲の開始キー
            end_key (bytes): 範囲の終了キー

        Returns:
            List[Tuple[bytes, bytes]]: 範囲内のキーと値のタプルのリスト
        """
        # ルートページを取得
        root_buffer = self.fetch_root_page(bufmgr)
        # 開始キーから範囲検索を開始
        return self.search_range_internal(bufmgr, root_buffer, start_key, end_key)

    def search_range_internal(self, bufmgr: BufferPoolManager, node_buffer: Buffer, start_key: bytes, end_key: bytes) -> List[Tuple[bytes, bytes]]:
        """
        再帰的に範囲検索を行う内部メソッド

        Args:
            bufmgr (BufferPoolManager): バッファプールマネージャ
            node_buffer (Buffer): 現在探索中のノードのバッファ
            start_key (bytes): 範囲の開始キー
            end_key (bytes): 範囲の終了キー

        Returns:
            List[Tuple[bytes, bytes]]: 範囲内のキーと値のタプルのリスト
        """
        node_type = struct.unpack('>I', node_buffer.page[:4])[0]
        results = []

        if node_type == NodeType.LEAF:
            # リーフノードの場合、ペアを取得して範囲内のペアを収集
            pairs = self.get_pairs(node_buffer)
            for pair in pairs:
                if start_key <= pair.key <= end_key:
                    results.append((pair.key, pair.value))
            return results
        else:
            # ブランチノードの場合、範囲内の子ノードを探索
            keys, children = self.get_branch(node_buffer)
            for i, key in enumerate(keys):
                if start_key < key:
                    # 指定された範囲内に含まれる子ノードを再帰的に探索
                    child_buffer = bufmgr.fetch_page(children[i])
                    results.extend(self.search_range_internal(bufmgr, child_buffer, start_key, end_key))
            # 最後の子ノードも探索
            child_buffer = bufmgr.fetch_page(children[-1])
            results.extend(self.search_range_internal(bufmgr, child_buffer, start_key, end_key))
            return results

# 実行部分
if __name__ == "__main__":
    import tempfile
    from buffer import BufferPool, BufferPoolManager
    from disk import DiskManager

    # 一時ファイルを作成
    with tempfile.NamedTemporaryFile(delete=False) as temp_file:
        temp_file_path = temp_file.name

    try:
        # ディスクマネージャとバッファプールの初期化
        disk = DiskManager.open(temp_file_path)  # ディスク上のファイルを開く
        pool = BufferPool(10)                    # バッファプールを初期化（最大10ページ）
        bufmgr = BufferPoolManager(disk, pool)   # バッファプールマネージャを作成

        # B+ツリーの作成
        btree = BPlusTree.create(bufmgr)

        # データ挿入テスト
        print("Inserting data into B+Tree...")
        btree.insert(bufmgr, struct.pack('>Q', 1), b"one")     # キー1を挿入
        btree.insert(bufmgr, struct.pack('>Q', 4), b"two")     # キー4を挿入
        btree.insert(bufmgr, struct.pack('>Q', 6), b"three")   # キー6を挿入
        btree.insert(bufmgr, struct.pack('>Q', 3), b"four")    # キー3を挿入
        btree.insert(bufmgr, struct.pack('>Q', 7), b"five")    # キー7を挿入
        btree.insert(bufmgr, struct.pack('>Q', 2), b"six")     # キー2を挿入
        btree.insert(bufmgr, struct.pack('>Q', 5), b"seven")   # キー5を挿入

        # データ検索テスト
        print("Searching data in B+Tree...")
        for key in [1, 2, 3, 4, 5, 6, 7]:
            key_bytes = struct.pack('>Q', key)  # キーをバイト列にパック
            result = btree.search(bufmgr, SearchMode.Key(key_bytes))  # 検索実行
            if result:
                found_key, value = result
                print(f"Key: {struct.unpack('>Q', found_key)[0]}, Value: {value.decode()}")
            else:
                print(f"Key {key} not found")

        # 範囲検索テスト（オプション）
        print("Searching range in B+Tree...")
        start_key = struct.pack('>Q', 2)
        end_key = struct.pack('>Q', 5)
        range_results = btree.search_range(bufmgr, start_key, end_key)
        for found_key, value in range_results:
            print(f"Range Key: {struct.unpack('>Q', found_key)[0]}, Value: {value.decode()}")

        print("B+Tree tests passed.")
    finally:
        # 一時ファイルを削除
        os.remove(temp_file_path)