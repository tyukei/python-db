# 挙動確認
```
db> CREATE TABLE phones
テーブル phones が作成されました。
db> SHOW TABLES
----- テーブル一覧 -----
phones
------------------------
db> select * from phones
レコードが見つかりません。
db> insert into phones values 1, google, white
レコードが挿入されました。
db> select * from phones
----- テーブル: phones -----
{'key': '1\x00\x00\x00\x00\x00\x00\x00\x01', 'value': 'google\x00\x00\x06white\x00\x00\x00\x05'}
```

キー
'1\x00\x00\x00\x00\x00\x00\x00\x01'
これは、ユーザーが入力した「1」が、内部で固定長（ここでは9バイト）にエンコードされた結果です。

最初の部分に「1」が入っており、その後、比較や整列を容易にするために余分な NULL バイト（\x00）で埋められています。
最後のバイト（\x01）は、元のデータの長さ（この場合は「1」の1バイト）を示すマーカーです。


値
'google\x00\x00\x06white\x00\x00\x00\x05'
これは、テーブルの値部分が2つのフィールドから構成されている場合の例です。具体的には：

最初のフィールド "google" は、6文字あるので、内部では以下のようにエンコードされます：

"google"（6バイト）
パディングとして 9-1-6 = 2 バイトの NULL (\x00\x00)

```
db> show btree phones
B+Tree 状態 (テーブル: phones):
  Meta Page ID: 13
  Root Page (先頭 64バイト):
0000000000000001
00000054
8004
95
49
00000000000000
8c05627472656594
8c045061697294
93942981947d9428
8c036b657994
430931
000000000000000194
```

Meta Page ID: 13
- 1~8バイト: ルートノートのページID
→ B+Tree全体の管理情報が保存されているページ番号が 13 である。

Root Page の先頭 64 バイト
仕様：
- 1~4バイト: ノードの種類（0=> リーフ、 1=> ブランチ）
- 5~8バイト: ペア数
- 9~: 実際のデータ
- 
00000000：8 バイトでノードタイプ（0 = リーフノード）を示す。
00000001：8 バイトでこのノードに1つのキー・値ペアがあることを示す。
00000054：8 バイトでペアデータのサイズが84バイトであることを示す。
8004:pickle ヘッダー。プロトコル 4 を使用している。
95: FRAME 命令のオペコード。これに続く8バイトがフレームサイズ。
49: FRAME 命令に続く8バイトのサイズ情報。
00000000000000: 区切り
8c04506169729493: btree
(8c→ pickle のオペコードで、短い文字列（SHORT_BINUNICODE や BINUNICODE）を示します。)
(94→ pickle の終端マーカーです。)
8c045061697294: Pair
93942981947d9428: pickle の内部命令や制御情報で、オブジェクトの構造
8c036b657994: key
430931: keyの値"1"
000000000000000194: keyのバイト数







```
db> insert into phones values 2, apple, red       
レコードが挿入されました。
db> select * from phones
----- テーブル: phones -----
{'key': '1\x00\x00\x00\x00\x00\x00\x00\x01', 'value': 'google\x00\x00\x06white\x00\x00\x00\x05'}
{'key': '2\x00\x00\x00\x00\x00\x00\x00\x01', 'value': 'apple\x00\x00\x00\x05red\x00\x00\x00\x00\x00\x03'}
db> show btree ascii phones
B+Tree ASCII Art for table: phones
Leaf: 1 | 2
db> show btree phones
B+Tree 状態 (テーブル: phones):
  Meta Page ID: 13
  Root Page (全内容):
0000000000000002
0000005480049549
00000000000000
8c05627472656594
8c045061697294
93942981947d9428
8c036b657994
430931
000000000000000194
8c0576616c756594
4312
676f6f676c65000006
7768697465
0000000594
75622e
0000005480049549
00000000000000
8c05627472656594
8c045061697294
93942981947d9428
8c036b657994
430932
000000000000000194
8c0576616c756594
4312
6170706c6500000005
726564
00000000000394
75622e
```

ASCII: 文字列　https://web-apps.nbookmark.com/ascii-converter/
8c05627472656594: btree
8c045061697294: Pair
8c036b657994: key
8c0576616c7565: value
676f6f676c65: google
7768697465: white
6170706c65: apple
726564: red
75622e: ub.

制御コード
0000000000000002：前半 4 バイト (00000000) はノードタイプ.次の 4 バイト (00000002) は、このノードに2つのペア
0000005480049549: pickle の FRAME 命令に関連するサイズやオフセット情報
00000000000000: 次のブロックへの区切り
93942981947d9428: pickle の内部命令や制御情報で、オブジェクトの構造
430931: keyの値が１
430932: keyの値が２
0000000594: 区切りを示す制御情報?
00000000000394: 区切りを示す制御情報?

## Pickle とは？
Pickle は、Python 標準のシリアライズモジュールです。
・シリアライズ：Python のオブジェクト（リスト、辞書、クラスインスタンスなど）をバイト列に変換し、ファイルやネットワーク経由で保存・送信できる形式にする。
・デシリアライズ：バイト列から元の Python オブジェクトを再構築する。

つまり、Pickle を使うと、複雑なデータ構造をそのままディスクに保存したり、別のプロセスやネットワーク越しに転送したりできるわけです。Pickle はプロトコル番号（ここでは 4 番など）を使って、内部のエンコード方式や圧縮（FRAME 命令など）を決めています。



## 1. table.py の詳細ドキュメント

### 概要  
`table.py` は、DBMS の高レベルインターフェースであるテーブル管理を行います。  
具体的には、レコードの挿入や表示（show）など、ユーザーからの操作を受け、内部では B+Tree などを駆使してデータを管理します。  
また、ユニークインデックスなどの補助構造もサポートしており、実際の RDBMS で求められる多様な機能をシンプルに実装しています。

### 各クラスとその役割  
- **SimpleTable**  
  テーブルを管理するクラス。  
  主に B+Tree を使ってレコードの挿入や検索、表示を行う。  
  また、キー部分と値部分を分割してエンコード／デコードする仕組みが組み込まれています。

- **UniqueIndex**  
  一意制約付きのインデックスを管理するクラス。  
  テーブル内のレコードのうち、特定フィールドの重複を防ぐために利用されます。

- **Table**  
  複数のインデックスを持つテーブルを管理するクラス。  
  SimpleTable よりも高機能な部分を担い、ユニークインデックスとの連携が可能。

### コード解説（行毎・詳細解説）

#### クラス SimpleTable

```python
class SimpleTable:
    def __init__(self, meta_page_id: PageId = None, num_key_elems: int = 0):
```
このコンストラクタでは、テーブル作成時に必要なメタデータのページ ID と、レコード内のキーとして使うフィールド数を受け取ります。  
ここでの `num_key_elems` は、レコードが [キー部分, 値部分] に分かれることを示し、たとえば最初の 1 要素がキー、残りが値といった構成を表現します。  
これは、後のレコードエンコードでキーと値を分離するために重要です。

```python
        self.meta_page_id = meta_page_id
        self.num_key_elems = num_key_elems
        self.btree = None  # 後で B+Tree インスタンスを格納
```
ここでは、メタページ ID とキー要素数をインスタンス変数に保持し、B+Tree のインスタンスを初期状態では None としています。  
B+Tree はテーブルの根幹となるデータ構造であり、create() メソッドで初期化されます。

```python
    def create(self, bufmgr: BufferPoolManager) -> None:
        self.btree = BPlusTree.create(bufmgr)
        self.meta_page_id = self.btree.meta_page_id
```
`create()` では、引数で渡されたバッファプールマネージャーを使って新たな B+Tree を作成し、そのメタデータページ ID を保持します。  
この段階で、実際の B+Tree 構造がディスク上（またはバッファ上）に確保され、後続の挿入処理が可能になります。

```python
    def insert(self, bufmgr: BufferPoolManager, record: list[bytes]) -> None:
```
このメソッドは、与えられたレコード（バイト列のリスト）をテーブルに挿入します。  
引数の `record` は、キー部と値部の両方を含むリストです。  
実際のデータベースでは、ここに複数フィールドが並びますが、シンプルなサンプルでは分割が明示されています。

```python
        btree = self.btree
        key = []
        tuple.encode(record[:self.num_key_elems], key)
```
ここで、レコードの先頭部分（キーとして使う部分）をエンコードしています。  
`tuple.encode` は、各フィールドをエンコードして整数のリスト（実際にはバイト列の連結結果）に変換する関数です。  
この処理により、キーが一意のバイト列となり、B+Tree での比較が可能になります。

```python
        value = []
        tuple.encode(record[self.num_key_elems:], value)
```
同様に、値部分もエンコードされます。  
キーと値を別々にエンコードすることで、B+Tree 内での管理が容易になります。

```python
        if not all(isinstance(item, bytes) for item in record):
            raise ValueError("All elements in the record must be of type bytes.")
```
ここでは、レコード内の各要素がバイト列であることをチェックしています。  
DBMS では型の整合性が非常に大事なので、このような検証処理を入れるのは安全設計の一環です。

```python
        key = bytes(key)
        value = bytes(value)
        btree.insert(bufmgr=bufmgr, key=key, value=value)
```
エンコードした結果をバイト列に変換し、B+Tree の挿入メソッドを呼び出します。  
この部分で、ディスクにデータが書き込まれる前の一連の処理が開始されます。

```python
    def show(self, bufmgr: BufferPoolManager):
        try:
            btree = self.btree
            results = btree.search_range(bufmgr, b'', b'\xff' * 16)
```
`show()` では、全件表示のために B+Tree の範囲検索を利用します。  
ここでは、開始キーを空文字、終了キーを非常に大きなバイト列（16 バイトのすべて 0xFF）に設定することで、全レコードを網羅するようにしています。  
このテクニックは、実際の DBMS でも全件スキャンを実現するための一般的手法です。

```python
            if not results:
                print("No records found.")
            else:
                for key, value in results:
                    record = []
                    tuple.decode(key, record)
                    tuple.decode(value, record)
                    print(tuple.Pretty(record))
            return True
```
検索結果が存在する場合、各レコードのキーと値をデコードし、`Pretty` クラスを使って人間が読みやすい形で表示します。  
各行に対して丁寧にデコード処理を行うのは、実際のデバッグやデータ確認において非常に有用です。

```python
        except Exception as e:
            print(f"Error: {e}")
```
例外処理により、予期しないエラーが発生した際もエラーメッセージを出力し、システムの堅牢性を確保しています。

### 雑学・背景知識  
- テーブル設計は、実際の RDBMS の基本中の基本です。  
  リレーショナルデータベースでは、各テーブルは行（レコード）と列（フィールド）から成り、キーを中心にデータが格納されます。  
- このコードはシンプルな設計ですが、実際にはトランザクション処理、ロック機構、障害復旧など多くの要素が加わります。  
- また、エンコード／デコード処理は、SQL の文字列比較やソート、インデックス作成の効率に直結するため、非常に緻密な設計が求められます。

---

## 2. btree.py の詳細ドキュメント

### 概要  
`btree.py` は、DBMS のコアともいえる B+Tree の実装を提供します。  
B+Tree は、データの高速な検索、挿入、範囲検索などを実現するための平衡木です。  
このファイルでは、リーフノードとブランチノードの管理、ノード分割、昇格処理、再帰的な挿入と検索が実装されています。  
また、Python の pickle を用いて Pair オブジェクトのシリアライズ／デシリアライズを行うなど、実装の細部にも工夫が光ります。

### 各クラスとその役割  
- **BPlusTree**  
  DBMS 内のデータ格納構造の根幹。  
  ノードの分割や昇格、再帰的な挿入／検索処理などを担当します。  
- **Pair**  
  リーフノードに格納される「キーと値」のペアを表すクラス。  
  シリアライズ可能なため、ページにバイナリ形式で保存できます。
- **SearchMode**  
  検索の開始方法（特定のキーによる検索か、先頭からの検索か）を管理するクラス。

### コード解説（行毎・詳細解説）

```python
import struct
from typing import Optional, Tuple, List
from buffer import BufferPoolManager, Buffer
from disk import PageId, PAGE_SIZE
import pickle
import os
```
ここでは、構造体パッキング、型ヒント、BufferPoolManager、DiskManager など他モジュールとの連携に必要なモジュールをインポートしています。  
特に pickle は、オブジェクトのシリアライズに欠かせないため、Pair クラスで使用します。

```python
class BTreeError(Exception):
    pass
```
B+Tree に特化した例外クラスを定義。  
実際のシステムでは、エラー発生時に適切なハンドリングをするために、特定の例外クラスを用意するのが一般的です。

```python
class DuplicateKeyError(BTreeError):
    pass
```
重複キー挿入時の例外。  
データベースではキーの一意性が重要なため、重複するキーを検出した際に例外を投げる設計は必須です。

```python
class NodeType:
    LEAF = 0    # リーフノード
    BRANCH = 1  # ブランチノード（内部ノード）
```
ノードの種類を定数として定義。  
リーフノードは実際のデータ（Pair）を持ち、ブランチノードは子ノードへのポインタとキー情報を持ちます。  
このように区別することで、ノード内のデータ処理が分岐されます。

```python
class SearchMode:
    START = 0
    KEY = 1
```
検索モードの定義。  
「START」は全件検索、「KEY」は特定のキーでの検索を表します。

```python
    def __init__(self, mode: int, key: Optional[bytes] = None):
        self.mode = mode
        self.key = key
```
ここでは、検索モードのインスタンスを作成。  
key が与えられた場合はそのキーで検索、そうでなければ先頭からの検索と判断されます。

```python
    @staticmethod
    def Start():
        return SearchMode(SearchMode.START)
```
先頭からの検索モードを簡単に生成できるように static method を用意しています。

```python
    @staticmethod
    def Key(key: bytes):
        return SearchMode(SearchMode.KEY, key)
```
特定のキーでの検索モードを生成するための補助メソッドです。

```python
class Pair:
    def __init__(self, key: bytes, value: bytes):
        self.key = key
        self.value = value
```
Pair クラスは、リーフノードに格納されるキーと値の組み合わせを表現します。  
各 Pair は後でシリアライズされ、ページ上に保存されるため、コンパクトに管理されます。

```python
    def to_bytes(self) -> bytes:
        return pickle.dumps(self)
```
`to_bytes` は、Pair オブジェクトをバイト列に変換するためのメソッド。  
pickle を使うことで、Python オブジェクトのシリアライズが簡単に行えます。  
ただし、セキュリティ面では注意が必要ですが、教材レベルではこの実装でも十分です。

```python
    @staticmethod
    def from_bytes(data: bytes) -> 'Pair':
        return pickle.loads(data)
```
逆に、バイト列から Pair オブジェクトを再構築するためのメソッドです。

```python
class BPlusTree:
    LEAF_NODE_MAX_PAIRS = 2
    BRANCH_NODE_MAX_KEYS = 2
```
ここで、各ノードの最大格納数を定数で定義しています。  
シンプルな実装のため、実際の DBMS ではこれらはもっと大きな値になりますが、教材用としては理解しやすくなっています。

```python
    def __init__(self, meta_page_id: PageId):
        self.meta_page_id = meta_page_id
```
BPlusTree インスタンスは、メタデータページ ID を保持します。  
このメタページには、ルートノードのページ ID が格納され、ツリー全体の入口となります。

```python
    @staticmethod
    def create(bufmgr: BufferPoolManager) -> 'BPlusTree':
        meta_buffer = bufmgr.create_page()
        root_buffer = bufmgr.create_page()
```
`create` メソッドでは、まずメタデータ用ページとルートノード用ページを作成します。  
これは、ツリー構造の初期化において非常に重要な工程です。

```python
        root_buffer.page[:4] = struct.pack('>I', NodeType.LEAF)
        root_buffer.page[4:8] = struct.pack('>I', 0)
```
ここで、ルートノードをリーフノードとして初期化し、ペア数を 0 に設定しています。  
struct.pack を用いて整数値をバイト列に変換し、ページ内の先頭に書き込んでいます。

```python
        meta_buffer.page[:8] = root_buffer.page_id.to_bytes()
```
メタページには、ルートノードのページ ID を記録します。  
これにより、ツリー全体へのアクセスが可能となります。

```python
        meta_buffer.is_dirty = True
        root_buffer.is_dirty = True
```
変更があったことを示すダーティフラグを設定。  
このフラグは、後でディスクへ変更内容を書き戻す際に重要です。

```python
        return BPlusTree(meta_page_id=meta_buffer.page_id)
```
B+Tree のインスタンスを返します。  
この時点で、ツリーの初期状態が確立されています。

以降、検索・挿入・ノード分割の各メソッドが定義されています。  
これらのメソッドは再帰的なアルゴリズムにより、ツリー内の正しい位置を探索したり、ノードのオーバーフロー時に分割を行ったりします。  
たとえば、`insert_internal()` では、リーフノードであれば新しい Pair を追加し、定数を超えた場合には `split_leaf()` を呼び出してノード分割を実施します。  
ブランチノードの場合も、適切な子ノードを選択し再帰呼び出しを行い、分割された場合には昇格キーを受け取って親ノードに反映します。

### 雑学・背景知識  
- B+Tree は実際のデータベースで最も広く採用されているインデックス構造です。  
  その理由は、ディスクアクセスの回数を最小限に抑え、範囲検索や順序付き検索に非常に適しているためです。  
- 分割処理や昇格キーの伝播は、ツリーのバランスを保ち、常に O(log N) の検索時間を保証するための重要な工夫です。  
- pickle を使ったシリアライズは、学習用としては便利ですが、本格的な実装では高速性や安全性の面で独自のシリアライズ方式が採用されることが多いです。

---

## 3. buffer.py の詳細ドキュメント

### 概要  
`buffer.py` は、ディスク I/O を効率化するためのバッファプールの実装を行います。  
ここでは、ディスクから読み込んだページをメモリ上のフレームとしてキャッシュし、頻繁なディスクアクセスを回避する仕組みが実装されています。  
また、ページ置換アルゴリズム（Clock-sweep 方式など）を利用して、キャッシュ内のフレームを効率的に管理します。

### 各クラスとその役割  
- **Buffer**  
  ページデータ（bytearray）、ページ ID、ダーティフラグなどを保持する基本単位です。
- **Frame**  
  Buffer をラップし、使用頻度（usage_count）など、置換アルゴリズムに必要な情報を管理します。
- **BufferPool**  
  複数の Frame をリストとして保持し、全体のキャッシュとして管理します。
- **BufferPoolManager**  
  BufferPool と DiskManager の橋渡しを行い、ページのフェッチ、作成、フラッシュなどを統括します。

### コード解説（行毎・詳細解説）

```python
import os
import struct
from collections import defaultdict
from typing import Dict, Optional, Tuple
from disk import DiskManager, PageId, PAGE_SIZE
```
ここでは、OS操作、バイナリ処理、型ヒント、そして DiskManager との連携に必要なモジュールをインポートしています。

```python
class BufferError(Exception):
    pass
```
バッファプールに関するエラーの基底クラスを定義。  
バッファ管理において問題が発生した場合、適切な例外処理を行えるようにしています。

```python
class NoFreeBufferError(BufferError):
    pass
```
利用可能なバッファフレームが見つからなかった場合に発生する例外。  
これは、ディスク I/O の遅延を避けるために非常に重要な概念です。

```python
class BufferId:
    def __init__(self, buffer_id: int):
        self.buffer_id = buffer_id
```
BufferId クラスは、バッファプール内のフレームを識別するための単純なラッパーです。  
各フレームに一意な ID を割り当て、ハッシュ可能なオブジェクトとして利用されます。

```python
    def __eq__(self, other):
        if isinstance(other, BufferId):
            return self.buffer_id == other.buffer_id
        return False

    def __hash__(self):
        return hash(self.buffer_id)
```
等価性とハッシュ値の計算を実装しており、バッファプール内での管理（辞書やセットでの利用）に対応しています。

```python
    def __repr__(self):
        return f"BufferId({self.buffer_id})"
```
デバッグ用に BufferId を文字列化するメソッドです。  
実際のシステムでは、ログ出力やデバッグ時に非常に有用です。

```python
class Buffer:
    def __init__(self, page_id: PageId):
        self.page_id = page_id
        self.page = bytearray(PAGE_SIZE)
        self.is_dirty = False
```
Buffer クラスは、ディスク上のページをメモリ上にキャッシュするための基本データ構造です。  
- `page_id`：ディスク上のページ番号。  
- `page`：実際のページデータ（bytearray、サイズ 4096 バイト）。  
- `is_dirty`：変更があったかどうかを示すフラグ。  
このフラグは、フラッシュ時にディスクに書き戻す必要があるかどうかを判断します。

```python
class Frame:
    def __init__(self, buffer: Buffer):
        self.usage_count = 0
        self.buffer = buffer
```
Frame は Buffer をラップし、使用頻度（usage_count）を管理します。  
使用頻度は、ページ置換アルゴリズムで重要な役割を果たします。  
たとえば、Clock-sweep では usage_count が 0 のフレームを選択するなどの工夫がされています。

```python
class BufferPool:
    def __init__(self, pool_size: int):
        self.buffers = [Frame(Buffer(PageId(PageId.INVALID_PAGE_ID))) for _ in range(pool_size)]
        self.next_victim_id = BufferId(0)
```
BufferPool は、指定された数（pool_size）の Frame をリストとして保持します。  
初期状態では、すべての Frame に無効なページ ID（INVALID_PAGE_ID）が設定されています。  
また、次に置換候補となるバッファ ID を `next_victim_id` として保持します。

```python
    def size(self) -> int:
        return len(self.buffers)
```
プールのサイズを返すシンプルなメソッドです。

```python
    def evict(self) -> Optional[BufferId]:
        pool_size = self.size()
        consecutive_pinned = 0
        while True:
            frame = self.buffers[self.next_victim_id.buffer_id]
            if frame.usage_count == 0:
                return self.next_victim_id
            if frame.buffer.is_dirty:
                frame.usage_count -= 1
                consecutive_pinned = 0
            else:
                consecutive_pinned += 1
                if consecutive_pinned >= pool_size:
                    return None
            self.next_victim_id = BufferId((self.next_victim_id.buffer_id + 1) % pool_size)
```
この `evict()` メソッドは、次に置換可能なバッファフレームを選択します。  
使用頻度（usage_count）が 0 のフレームを返すようになっており、dirty な場合は usage_count を減少させる工夫があります。  
全フレームを一巡しても見つからなければ None を返し、十分な空きがない状況を示します。  
このアルゴリズムは、実際の DBMS でも採用される Clock-sweep アルゴリズムの一種です。

```python
class BufferPoolManager:
    def __init__(self, disk: DiskManager, pool: BufferPool):
        self.disk = disk
        self.pool = pool
        self.page_table: Dict[PageId, BufferId] = {}
```
BufferPoolManager は、ディスクと BufferPool を統括する役割を持ちます。  
ここで `page_table` は、ディスクのページ ID と BufferPool 内の BufferId のマッピングを保持し、どのページがどこにあるかを迅速に検索できるようにします。

```python
    def fetch_page(self, page_id: PageId) -> Buffer:
```
`fetch_page()` は、指定されたページ ID のページをメモリ上に確保し、Buffer を返します。  
既にキャッシュにあれば再利用し、なければ evict() で空きフレームを探し、ディスクから読み込みます。

```python
    def create_page(self) -> Buffer:
```
`create_page()` は、新たにページをディスク上に割り当て、バッファプールに新しい Buffer として登録するためのメソッドです。  
内部では、evict() を利用してフレームを確保し、DiskManager の allocate_page() を呼び出して新ページを生成します。

```python
    def flush(self) -> None:
```
`flush()` では、バッファプール内のすべての dirty ページをディスクに書き戻し、DiskManager の sync() を呼び出して物理ディスクへの同期を保証します。  
これにより、メモリ上の変更が確実に永続化されます。

### 雑学・背景知識  
- バッファプールの概念は、現代の DBMS の性能向上において極めて重要な部分です。  
  実際には数百～数千のページが同時にメモリ上に保持され、キャッシュのヒット率を最大化することがシステム性能の鍵となります。  
- 置換アルゴリズムとしては、LRU（Least Recently Used）や Clock-sweep、さらには ARC（Adaptive Replacement Cache）などがあり、ここではシンプルな Clock-sweep に近い実装を採用しています。  
- ページテーブルは、ハッシュテーブルとして実装されることが多く、高速な検索が可能です。  
  この考え方は、オペレーティングシステムのページテーブルにも似た概念があり、メモリ管理全般に共通するアイデアです。

---

## 4. disk.py の詳細ドキュメント

### 概要  
`disk.py` は、ディスク上のページを管理するためのモジュールです。  
ここでは、ヒープファイルという単一ファイル上にデータを格納し、ページ単位で読み書きする仕組みを実装しています。  
この実装は、実際のデータベースが行うディスク I/O の基本をシンプルに再現しており、非常に重要な役割を果たします。

### 各クラスとその役割  
- **PageId**  
  ページを一意に識別するためのクラス。  
  ページ ID は 64 ビット整数で管理され、無効なページを示す INVALID_PAGE_ID も定義されています。
- **DiskManager**  
  ヒープファイルを通じて、ページの読み書き、ページの割り当て、同期処理を行います。  
  これは、DBMS の永続化層を担う重要なコンポーネントです。

### コード解説（行毎・詳細解説）

```python
import os
import struct
from typing import Optional
```
ここでは、OS のファイル操作、バイナリデータのパック・アンパック、型ヒントをインポートしています。

```python
PAGE_SIZE = 4096
```
ページサイズを 4096 バイト（4KB）に固定。  
多くのファイルシステム（例: ext4）ではブロックサイズが 4KB であるため、これに合わせています。  
実際のシステムではこのサイズはハードウェアに依存することもあり、パフォーマンスに大きな影響を与えます。

```python
class PageId:
    INVALID_PAGE_ID = 2**64 - 1
```
PageId クラスの定義開始。  
`INVALID_PAGE_ID` は、無効なページを示すための定数であり、実際のページ番号がこの値に達することは通常ありません。

```python
    def __init__(self, page_id: int):
        self.page_id = page_id
```
コンストラクタでは、整数型のページ ID を受け取り、インスタンス変数に保持します。  
非常にシンプルですが、後でページ ID をバイト列に変換するなどの操作に利用されます。

```python
    def to_u64(self) -> int:
        return self.page_id
```
ページ ID を 64 ビットの整数として返すメソッドです。  
この値は、ディスク上のオフセット計算などに使われます。

```python
    @staticmethod
    def from_bytes(bytes_data: bytes) -> 'PageId':
        page_id, = struct.unpack('Q', bytes_data)
        return PageId(page_id)
```
バイト列から PageId オブジェクトを生成する静的メソッド。  
ここでは、struct.unpack を使ってバイト列から整数を取り出し、新しい PageId を作成します。  
この処理は、ディスクから読み込んだメタデータの解析時に重要です。

```python
    def to_bytes(self) -> bytes:
        return struct.pack('Q', self.page_id)
```
逆に、PageId をバイト列に変換するメソッド。  
ディスクへの書き込み時にページ ID を記録するために使用されます。

```python
    def __eq__(self, other):
        if isinstance(other, PageId):
            return self.page_id == other.page_id
        return False
```
PageId 同士の等価性を比較するための実装です。  
これにより、同じページ ID を持つオブジェクトは同一視されます。

```python
    def __hash__(self):
        return hash(self.page_id)
```
ハッシュ関数を定義することで、PageId を辞書のキーとして利用できるようにしています。

```python
    def __repr__(self):
        return f"PageId({self.page_id})"
```
デバッグやログ出力用に、PageId を見やすい文字列に変換するメソッドです。

```python
class DiskManager:
    def __init__(self, heap_file: str): 
        self.heap_file = heap_file
        self.file = open(heap_file, 'r+b')
        self.file.seek(0, os.SEEK_END)
        self.next_page_id = self.file.tell() // PAGE_SIZE
```
DiskManager のコンストラクタ。  
- `heap_file` は、データを格納するヒープファイルのパス。  
- ファイルを読み書きモードでオープンし、ファイルサイズから次に使用すべきページ ID を算出します。  
この実装はシンプルですが、実際にはファイルのメタデータやフリーリストを管理する必要があります。

```python
    @staticmethod
    def open(heap_file_path: str) -> 'DiskManager':
        if not os.path.exists(heap_file_path):
            with open(heap_file_path, 'w+b') as f:
                pass
        return DiskManager(heap_file_path)
```
ヒープファイルが存在しなければ新規作成し、DiskManager のインスタンスを返す静的メソッド。  
これにより、初回実行時のファイル存在チェックが自動的に行われます。

```python
    def read_page_data(self, page_id: PageId, data: bytearray) -> None:
        offset = PAGE_SIZE * page_id.to_u64()
        self.file.seek(offset)
        self.file.readinto(data)
```
指定されたページ ID の位置にシークし、ページデータを bytearray に読み込むメソッドです。  
オフセット計算にはページサイズを乗じ、非常に効率的な一括読み込みを行います。

```python
    def write_page_data(self, page_id: PageId, data: bytes) -> None:
        offset = PAGE_SIZE * page_id.to_u64()
        self.file.seek(offset)
        self.file.write(data)
```
同様に、指定されたページ ID の位置にシークし、データを書き込むメソッドです。  
ディスク I/O の基本中の基本であり、実際の DBMS ではバッファ管理との連携が求められます。

```python
    def allocate_page(self) -> PageId:
        page_id = self.next_page_id
        self.next_page_id += 1
        return PageId(page_id)
```
新しいページを割り当てるためのメソッドです。  
シンプルなカウンター方式ですが、実際のシステムではフリーリスト管理が行われる場合もあります。

```python
    def sync(self) -> None:
        self.file.flush()
        os.fsync(self.file.fileno())
```
ファイルの内容をディスクに同期させるメソッド。  
データの永続性を保証するため、フラッシュと fsync を組み合わせて使用しています。

```python
    def __del__(self):
        self.file.close()
```
オブジェクト破棄時にファイルを閉じるデストラクタ。  
リソース管理のために重要な部分です。

### 雑学・背景知識  
- ディスク I/O は DBMS のパフォーマンスに大きな影響を与えるため、Page サイズの選定やバッファ管理、ディスクスケジューリングなど、非常に多くの研究が行われています。  
- 実際のシステムでは、SSD や NVMe の登場により、従来の HDD とは異なる最適化が必要となりますが、基本概念はこのコードと同様です。

---

## 5. tuple.py の詳細ドキュメント

### 概要  
`tuple.py` は、テーブルの各レコード（タプル）を構成するフィールドのエンコード／デコードを行うモジュールです。  
データベース内部では、各フィールドがバイト列で表現され、比較やソートが容易な形にエンコードされます。  
このファイルでは、`memcmpable` モジュールの関数を利用し、各フィールドを固定サイズブロックに変換する処理が実装されています。

### 各関数の役割  
- **encode**  
  与えられたイテレータ（バイト列）から、各要素をエンコードし、整数リスト（実際にはバイト値の連結）に追加する。
- **decode**  
  バイト列からエンコードされたデータを復元し、元のバイト列のリストとして返す。
- **Pretty**  
  デバッグ用にタプルを人間が読みやすい形式に変換するクラス。  
  例えば、UTF-8 でデコード可能な文字列はダブルクォーテーションで囲み、さらにそのバイト列の16進数表現も表示する。

### コード解説（行毎・詳細解説）

```python
from typing import Iterator, List
import memcmpable
import fmt
```
まず、型ヒント用の Iterator や List、そして memcmpable や fmt という補助モジュールをインポートしています。  
`memcmpable` はエンコード／デコード処理を実装しており、`fmt` はフォーマット（Pretty 表示）用の補助関数を含むと考えられます。

```python
ESCAPE_LENGTH = 9
```
エスケープ長を定数として定義。  
この値は、固定ブロックのサイズを表し、実際にデータをブロック単位で処理する際の基準となります。

```python
def encode(elems: Iterator[bytes], bytes_list: List[int]) -> None:
    for elem in elems:
        elem_bytes = elem
        length = memcmpable.encoded_size(len(elem_bytes))
        memcmpable.encode(elem_bytes, bytes_list)
```
`encode` 関数は、各要素（バイト列）を受け取り、`memcmpable.encoded_size` で必要なサイズを計算し、`memcmpable.encode` を用いてエンコード処理を行います。  
この処理により、元のデータが固定長のブロックに整形され、後の比較やソートが容易になります。  
各行は、エンコードの流れをシンプルに記述し、不要な操作を排除した設計となっています。

```python
def decode(bytes_data: bytes, elems: List[bytes]) -> None:
    rest = bytes_data
    while rest:
        elem, rest = memcmpable.decode(rest)
        elems.append(elem)
```
`decode` 関数は、与えられたバイト列から、`memcmpable.decode` を呼び出し、エンコードされた各フィールドを復元してリストに追加します。  
while ループを使って、全データが消費されるまで処理を繰り返すのが特徴です。

```python
class Pretty:
    def __init__(self, data: List[bytes]):
        self.data = data
```
`Pretty` クラスは、デバッグ用にタプルの各フィールドを人間に分かりやすい形で表示するためのクラスです。  
コンストラクタでデータリストを保持します。

```python
    def __repr__(self) -> str:
        debug_tuple = "Tuple("
        fields = []
        for elem in self.data:
            try:
                s = elem.decode('utf-8')
                fields.append(f'"{s}" {elem.hex()}')
            except UnicodeDecodeError:
                fields.append(f'{elem.hex()}')
        debug_tuple += ", ".join(fields) + ")"
        return debug_tuple
```
`__repr__` メソッドでは、各フィールドを UTF-8 でデコードを試み、成功すれば文字列として表示し、失敗すれば 16 進数表記で表示します。  
この方法により、バイナリデータや文字列データのどちらも適切に可視化でき、デバッグやログ出力で非常に役立ちます。

### 雑学・背景知識  
- タプルのエンコード／デコード処理は、実際のデータベースのインデックスやソート処理において不可欠です。  
  固定長にすることで、単純なバイト列の比較が可能となり、効率的な B+Tree 構築が実現されます。  
- memcmpable という名称は、「メモリ上で比較可能」という意味であり、C言語の memcmp 関数のような低レベルの比較機能に由来する考え方です。  
- fmt というモジュールは、フォーマット出力のための補助関数群であり、実際の DBMS のデバッグツールとしても重要な役割を果たします。

---

## 6. query.py の詳細ドキュメント

### 概要  
`query.py` は、SQL クエリの実行やタプルのエンコード／デコードを補助するモジュールとして設計されています。  
基本的には `tuple.py` と似た機能を持ちますが、こちらはよりクエリ処理を意識した設計がなされていると考えられます。  
例えば、エンコード時にリザーブ機能（reserve）を呼び出すなど、パフォーマンスや効率面において工夫がされています。

### 各関数の役割  
- **encode**  
  タプルを構成するバイト列をエンコードし、バッファ（整数リスト）に追加する。  
- **decode**  
  エンコードされたバイト列を復元し、元のタプルとしてリストに追加する。
- **Pretty**  
  デバッグや表示用にタプルを整形して返すクラス。
- **encoded_size, encode_data, decode_data**  
  エンコードサイズの計算、データのエンコード／デコードの低レベル処理を実装しています。

### コード解説（行毎・詳細解説）

```python
from typing import Iterator, List
import fmt
import memcmpable
import cmp
```
型ヒントや、fmt、memcmpable、cmp といった補助モジュールをインポート。  
`cmp` は比較関数などを提供する可能性があり、エンコード後のバイト列比較に役立ちます。

```python
ESCAPE_LENGTH = 9
```
`tuple.py` と同様に、エスケープ長を定数として定義。  
全体のエンコード処理のブロックサイズを統一するためのものです。

```python
def encode(elems: Iterator[bytes], bytes_list: List[int]) -> None:
    for elem in elems:
        elem_bytes = elem
        length = memcmpable.encoded_size(len(elem_bytes))
        bytes_list.reserve(length)
        memcmpable.encode(elem_bytes, bytes_list)
```
ここでは、各要素をエンコードする際に、バッファリストに十分なスペースを確保（reserve）する処理が追加されています。  
これは、パフォーマンス面で連続した拡張を防ぐための工夫です。  
その後、memcmpable.encode を呼び出し、実際のエンコード処理を委譲しています。

```python
def decode(bytes_data: bytes, elems: List[bytes]) -> None:
    rest = bytes_data
    while rest:
        elem, rest = memcmpable.decode(rest)
        elems.append(elem)
```
基本的な decode 関数は、残りのデータがなくなるまで memcmpable.decode を呼び出し、得られたフィールドをリストに追加します。

```python
class Pretty:
    def __init__(self, data: List[bytes]):
        self.data = data
```
Pretty クラスは、タプルを人間が読みやすい形式に整形して表示するためのものです。  
ここでは、単純に内部データを保持するだけです。

```python
    def __repr__(self) -> str:
        debug_tuple = "Tuple("
        fields = []
        for elem in self.data:
            try:
                s = elem.decode('utf-8')
                fields.append(f'"{s}" {elem.hex()}')
            except UnicodeDecodeError:
                fields.append(f'{elem.hex()}')
        debug_tuple += ", ".join(fields) + ")"
        return debug_tuple
```
__repr__ では、各フィールドを UTF-8 でデコードし、成功すれば文字列と 16 進数表現を、失敗すれば 16 進数のみを表示。  
この詳細な表示は、開発時に各フィールドの内容を正確に把握するために有用です。

```python
def encoded_size(length: int) -> int:
    return ((length + (ESCAPE_LENGTH - 1)) // (ESCAPE_LENGTH - 1)) * ESCAPE_LENGTH
```
encoded_size 関数は、元データの長さからエンコード後のサイズを計算します。  
この計算式は、ブロック単位の切り上げを行うためのものです。

```python
def encode_data(src: bytes, dst: List[int]) -> None:
    while True:
        copy_len = min(ESCAPE_LENGTH - 1, len(src))
        dst.extend(src[:copy_len])
        src = src[copy_len:]
        if not src:
            pad_size = ESCAPE_LENGTH - 1 - copy_len
            if pad_size > 0:
                dst.extend([0] * pad_size)
            dst.append(copy_len)
            break
```
encode_data では、src バイト列をエスケープ長のブロックに分割し、各ブロックの最後に実際にコピーした長さを記録します。  
これにより、可変長データを固定ブロックで管理でき、バイト列の比較が容易になります。

```python
def decode_data(src: bytes, dst: bytearray) -> None:
    while src:
        extra = src[ESCAPE_LENGTH - 1]
        length = min(ESCAPE_LENGTH - 1, extra)
        dst.extend(src[:length])
        src = src[ESCAPE_LENGTH:]
        if extra < ESCAPE_LENGTH:
            break
```
decode_data では、エンコードされた src を解析し、各ブロックからデータを抽出して dst に追加します。  
extra（最後のバイト）の値を用いて、実際のデータ長を決定しています。

```python
def test():
    org1 = b"helloworld!memcmpable"
    org2 = b"foobarbazhogehuga"
    enc = []
    encode(org1, enc)
    encode(org2, enc)
    rest = bytes(enc)
    dec1 = bytearray()
    decode(rest, dec1)
    assert org1 == bytes(dec1)
    dec2 = bytearray()
    decode(rest, dec2)
    assert org2 == bytes(dec2)
```
test 関数は、エンコード／デコード処理が正しく動作するかを確認するための簡単なユニットテストです。  
このようなテストは、実際のシステム開発において必須の工程です。

### 雑学・背景知識  
- クエリ処理やタプルの管理において、エンコード／デコードの設計は非常に重要です。  
  固定ブロック方式は、ディスク上のデータ比較やソートを効率化するために広く使われています。  
- reserve の概念は、C++ の vector の reserve() に似た発想で、メモリ再確保の回数を減らし、パフォーマンスを向上させる狙いがあります。  
- このような低レベルのデータ変換処理は、システム全体の性能に直結するため、実際の DBMS 開発でも非常に重要な領域です。

---

## 7. memcmpable.py の詳細ドキュメント

### 概要  
`memcmpable.py` は、データを比較可能な形式にエンコード／デコードするための低レベルモジュールです。  
名前の通り、メモリ上での比較（memcmp）が容易になるように、可変長データを固定長ブロックに変換する仕組みを実装しています。  
この仕組みは、B+Tree でのキー比較や、タプルのソートにおいて不可欠な技術です。

### 各関数の役割  
- **encoded_size**  
  入力データの長さから、エンコード後に必要となるブロック数を計算します。
- **encode**  
  入力のバイト列を、ESCAPE_LENGTH バイト単位のブロックに分割し、各ブロック末尾に実際の使用バイト数を記録します。
- **decode**  
  エンコードされたバイト列から、元のデータを復元し、残りのバイト列を返す。

### コード解説（行毎・詳細解説）

```python
from typing import Iterator, List, Tuple
```
型ヒントとして、Iterator、List、Tuple をインポートしています。  
これにより、関数の戻り値や引数の型が明確になり、可読性が向上します。

```python
ESCAPE_LENGTH = 9
```
定数として、各ブロックのサイズ（9 バイト）を定義。  
この値は、データのブロック単位の管理において中心的な役割を果たします。

```python
def encoded_size(length: int) -> int:
    return ((length + (ESCAPE_LENGTH - 1)) // (ESCAPE_LENGTH - 1)) * ESCAPE_LENGTH
```
この関数は、元データの長さから、エンコード後に必要となる総バイト数を計算します。  
整数の切り上げ計算を行うことで、部分的なブロックがあっても十分な領域が確保されるようにしています。

```python
def encode(src: bytes, dst: List[int]) -> None:
    while True:
        copy_len = min(ESCAPE_LENGTH - 1, len(src))
        dst.extend(src[:copy_len])
        src = src[copy_len:]
        if not src:
            pad_size = ESCAPE_LENGTH - 1 - copy_len
            if pad_size > 0:
                dst.extend([0] * pad_size)
            dst.append(copy_len)
            break
        dst.append(ESCAPE_LENGTH)
```
このエンコード関数は、以下のステップで動作します：  
1. 入力データ `src` から、最大で `ESCAPE_LENGTH - 1` バイトをコピーします。  
2. コピーしたバイト数が足りない場合は、パディング（0埋め）を行います。  
3. 最後のバイトには、実際にコピーしたバイト数（もしくは次のブロックがある場合は ESCAPE_LENGTH）を追加します。  
これにより、後で正確に復元できる固定長ブロックが作成されます。

```python
def decode(src: bytearray, dst: bytearray) -> None:
    while src:
        extra = src[ESCAPE_LENGTH - 1]
        length = min(ESCAPE_LENGTH - 1, extra)
        dst.extend(src[:length])
        del src[:ESCAPE_LENGTH]
        if extra < ESCAPE_LENGTH:
            break
```
この decode 関数は、エンコードされた `src` からブロック単位でデータを抽出し、`dst` に追加します。  
- 各ブロックの最後のバイト（marker）を見て、実際にデータが存在する長さを決定します。  
- ブロック単位で `src` からデータを削除（del）し、残りの部分に対して同じ処理を繰り返します。  
- marker が ESCAPE_LENGTH 未満の場合、ブロックの終端と判断し、処理を終了します。

```python
def test():
    org1 = b"helloworld!memcmpable"
    org2 = b"foobarbazhogehuga"
    enc = []
    encode(org1, enc)
    encode(org2, enc)
    rest = bytearray(enc)
    dec1 = bytearray()
    decode(rest, dec1)
    assert org1 == bytes(dec1), "デコードされたデータが一致しません。"
    dec2 = bytearray()
    decode(rest, dec2)
    assert org2 == bytes(dec2), "デコードされたデータが一致しません。"
    print("すべてのテストが成功しました。")
```
test 関数は、エンコードと decode の正当性を確認するためのユニットテストです。  
２つの異なるバイト列をエンコードして、正確に復元できるかを assert 文で検証しています。

### 雑学・背景知識  
- このエンコード方式は、シンプルながらも「可変長データを固定長ブロックで扱う」という重要な考え方を実現しています。  
- 多くの DBMS では、内部データの比較を高速に行うため、固定長のキー形式が求められます。  
- memcmpable という名前は、C言語の memcmp 関数に由来しており、メモリ上でのバイト列比較を容易にする設計思想を反映しています。  
- このような低レベルの実装は、実際のデータベースシステムにおいてもパフォーマンスの鍵となるため、しっかりと理解しておくべき重要な技術です。
