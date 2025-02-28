import sys
from buffer import BufferPool, BufferPoolManager
from disk import DiskManager, PageId
from btree import BPlusTree, SearchMode
from table import SimpleTable  # 先に実装済みの SimpleTable を利用
import struct

def main():
    try:
        # ★ディスクマネージャの初期化
        # ヒープファイル "simple.rly" をオープン（存在しなければ作成）
        disk = DiskManager.open("simple.rly")
        
        # ★バッファプールの作成
        # シンプルな実装ではプールサイズを10に設定
        pool = BufferPool(10)
        
        # ★バッファプールマネージャの作成
        # DiskManagerとBufferPoolを統合管理する
        bufmgr = BufferPoolManager(disk, pool)
        
        # テーブルはまだ存在していない状態
        table = None
        
        print("===== Simple DBMS Shell =====")
        print("利用可能なコマンド:")
        print("  CREATE TABLE       - 新しいテーブルを作成")
        print("  INSERT <値1>,<値2>,...  - テーブルにレコードを挿入（カンマ区切り）")
        print("  SHOW               - テーブルの全レコードを表示")
        print("  DELETE <キー>      - キーに該当するレコードを削除 (※未実装)")
        print("  DROP TABLE         - テーブルを削除")
        print("  EXIT               - シェルを終了")
        print("============================")
        
        # 対話型シェルのループ
        while True:
            # ユーザーからコマンドを受け取る
            cmd = input("db> ").strip()
            if not cmd:
                continue

            # コマンドの小文字化（比較用）
            cmd_lower = cmd.lower()
            
            # 終了コマンド
            if cmd_lower in ["exit", "quit"]:
                print("終了します...")
                break
            
            # テーブル作成コマンド：例 "create table"
            elif cmd_lower.startswith("create table"):
                # ここではテーブル名は無視してシンプルにテーブルを作成
                table = SimpleTable(meta_page_id=PageId(0), num_key_elems=1)
                table.create(bufmgr)
                print("テーブルが作成されました。")
            
            # レコード挿入コマンド：例 "insert Alice,Smith,30"
            elif cmd_lower.startswith("insert"):
                if table is None:
                    print("エラー: まずテーブルを作成してください。")
                    continue
                try:
                    # "insert" キーワードの後の部分を取得
                    # 値はカンマ区切りで指定。例：insert John,Doe,25
                    values_part = cmd[len("insert"):].strip()
                    if not values_part:
                        print("挿入するレコードの値を指定してください。")
                        continue
                    # カンマ区切りに分解し、各値を前後の空白を除去
                    parts = [v.strip() for v in values_part.split(",") if v.strip()]
                    # ここでは各フィールドを utf-8 にエンコードしてバイト列に変換
                    record = [v.encode("utf-8") for v in parts]
                    table.insert(bufmgr, record)
                    print("レコードが挿入されました。")
                except Exception as e:
                    print(f"挿入エラー: {e}")
            
            # テーブル内容表示コマンド：例 "show"
            elif cmd_lower == "show":
                if table is None:
                    print("エラー: テーブルが存在しません。")
                    continue
                print("----- テーブル内容 -----")
                table.show(bufmgr)
                print("------------------------")
            
            # レコード削除コマンド：例 "delete John"
            elif cmd_lower.startswith("delete"):
                # ※現状、シンプルなサンプルでは削除機能は実装していません
                # 実際には、B+Treeから指定キーを探して削除する処理が必要です
                key_to_delete = cmd[len("delete"):].strip()
                if not key_to_delete:
                    print("削除するキーを指定してください。")
                    continue
                # ここは未実装としてメッセージを表示
                print("削除機能は現在未実装です。")
            
            # テーブル削除コマンド：例 "drop table"
            elif cmd_lower.startswith("drop table"):
                # テーブルを削除する場合は、シンプルに table オブジェクトを None にする
                table = None
                print("テーブルが削除されました。")
            
            else:
                print("不明なコマンドです。利用可能なコマンドを確認してください。")
        
        # シェル終了前に、すべてのバッファをディスクにフラッシュ
        bufmgr.flush()
    
    except Exception as e:
        print(f"エラーが発生しました: {e}")

if __name__ == "__main__":
    main()
