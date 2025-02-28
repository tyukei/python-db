
import sys
import json
import os
from buffer import BufferPool, BufferPoolManager
from disk import DiskManager, PageId
from table import SimpleTable
from btree import BPlusTree, SearchMode
import struct
import tuple

class QueryEngine:
    def __init__(self, heap_file="simple.rly", pool_size=10, schema_file="schema.json"):
        """
        QueryEngine の初期化。
        
        :param heap_file: ヒープファイルのパス。
        :param pool_size: バッファプールのフレーム数。
        :param schema_file: テーブル定義を保存するファイル名。
        """
        self.disk = DiskManager.open(heap_file)
        self.pool = BufferPool(pool_size)
        self.bufmgr = BufferPoolManager(self.disk, self.pool)
        self.tables = {}  # {テーブル名: SimpleTable}
        self.schema_file = schema_file
        self.load_schema()

    def load_schema(self):
        """
        schema_file が存在すれば、テーブル定義を読み込み、self.tables に再構築する。
        """
        if os.path.exists(self.schema_file):
            try:
                with open(self.schema_file, "r") as f:
                    schema = json.load(f)
                for table_name, info in schema.items():
                    meta_page_id_int = info.get("meta_page_id", 0)
                    num_key_elems = info.get("num_key_elems", 1)
                    table = SimpleTable(meta_page_id=PageId(meta_page_id_int), num_key_elems=num_key_elems)
                    # ※シンプルな例なので、B+Tree の完全な復元は行わず、meta_page_id だけを使って新規作成
                    table.btree = BPlusTree(meta_page_id=PageId(meta_page_id_int))
                    self.tables[table_name] = table
                print("スキーマがロードされました。")
            except Exception as e:
                print(f"スキーマ読み込みエラー: {e}")
        else:
            self.tables = {}

    def save_schema(self):
        """
        現在のテーブル定義を schema_file に保存する。
        """
        schema = {}
        for table_name, table in self.tables.items():
            schema[table_name] = {
                "meta_page_id": table.meta_page_id.page_id,
                "num_key_elems": table.num_key_elems
            }
        try:
            with open(self.schema_file, "w") as f:
                json.dump(schema, f)
        except Exception as e:
            print(f"スキーマ保存エラー: {e}")

    def list_tables(self):
        """
        現在存在するテーブル名の一覧を返す。
        """
        return list(self.tables.keys())

    def create_table(self, table_name, num_key_elems=1):
        if table_name in self.tables:
            raise Exception(f"テーブル {table_name} は既に存在します。")
        table = SimpleTable(meta_page_id=PageId(0), num_key_elems=num_key_elems)
        table.create(self.bufmgr)
        self.tables[table_name] = table
        self.save_schema()
        return f"テーブル {table_name} が作成されました。"

    def drop_table(self, table_name):
        if table_name not in self.tables:
            raise Exception(f"テーブル {table_name} は存在しません。")
        del self.tables[table_name]
        self.save_schema()
        return f"テーブル {table_name} が削除されました。"

    def insert_into(self, table_name, values):
        if table_name not in self.tables:
            raise Exception(f"テーブル {table_name} は存在しません。")
        table = self.tables[table_name]
        record = [str(v).encode("utf-8") for v in values]
        table.insert(self.bufmgr, record)
        return "レコードが挿入されました。"

    def select_from(self, table_name, where_key=None, show_hash=False):
        if table_name not in self.tables:
            raise Exception(f"テーブル {table_name} は存在しません。")
        table = self.tables[table_name]
        results = table.btree.search_range(self.bufmgr, b'', b'\xff'*16)
        filtered_results = []

        # WHERE 句が指定されている場合、ユーザー入力キーを内部形式にエンコードする
        if where_key is not None:
            encoded_where = []
            tuple.encode([where_key.encode("utf-8")], encoded_where)
            where_key_encoded = bytes(encoded_where)
        else:
            where_key_encoded = None

        for key, value in results:
            if where_key_encoded is not None and key != where_key_encoded:
                continue

            try:
                key_str = key.decode("utf-8", errors="replace")
            except Exception:
                key_str = str(key)
            try:
                value_str = value.decode("utf-8", errors="replace")
            except Exception:
                value_str = str(value)
            record = {"key": key_str, "value": value_str}
            if show_hash:
                record["hash_key"] = hash(key_str)
                record["hash_value"] = hash(value_str)
            filtered_results.append(record)
        return filtered_results

    def flush(self):
        self.bufmgr.flush()

    def show_btree(self, table_name):
        """
        B+Tree の状態を可視化する簡易ダンプを表示する（16進数出力）。
        """
        if table_name not in self.tables:
            raise Exception(f"テーブル {table_name} は存在しません。")
        table = self.tables[table_name]
        root_buffer = table.btree.fetch_root_page(self.bufmgr)
        print(f"B+Tree 状態 (テーブル: {table_name}):")
        print(f"  Meta Page ID: {table.btree.meta_page_id.page_id}")
        print("  Root Page (先頭 64バイト):")
        print(root_buffer.page[:64].hex())

    def show_page(self, page_id_int):
        """
        指定したページIDの内容を表示する。
        """
        try:
            page_id = PageId(int(page_id_int))
            buffer = self.bufmgr.fetch_page(page_id)
            print(f"ページ {page_id_int} の内容 (16進数):")
            print(buffer.page.hex())
        except Exception as e:
            print(f"ページ表示エラー: {e}")

    def print_btree_ascii(self, table_name):
        """
        指定したテーブルのB+Tree構造をアスキーアートで表示する。
        """
        if table_name not in self.tables:
            raise Exception(f"テーブル {table_name} は存在しません。")
        table = self.tables[table_name]
        print(f"B+Tree ASCII Art for table: {table_name}")
        root_buffer = table.btree.fetch_root_page(self.bufmgr)
        self._print_btree_node(root_buffer, table, "")

    def _print_btree_node(self, node_buffer, table, indent):
        """
        再帰的にB+Treeのノードを表示するヘルパー関数。
        
        :param node_buffer: 現在のノードのバッファ
        :param table: 対象テーブルの SimpleTable インスタンス
        :param indent: 表示用のインデント文字列
        """
        node_type = struct.unpack('>I', node_buffer.page[:4])[0]
        if node_type == 0:  # リーフノード
            pairs = table.btree.get_pairs(node_buffer)
            keys = [p.key.decode("utf-8", errors="replace") for p in pairs]
            print(indent + "Leaf: " + " | ".join(keys))
        else:
            keys, children = table.btree.get_branch(node_buffer)
            keys_str = [k.decode("utf-8", errors="replace") for k in keys]
            print(indent + "Branch: " + " | ".join(keys_str))
            for child in children:
                child_buffer = self.bufmgr.fetch_page(child)
                self._print_btree_node(child_buffer, table, indent + "    ")

    def run_shell(self):
        def print_help():
            print("===== DBMS Interactive Shell Help =====")
            print("利用可能なコマンド:")
            print("  CREATE TABLE <table_name>")
            print("  DROP TABLE <table_name>")
            print("  SHOW TABLES")
            print("  INSERT INTO <table_name> VALUES <val1>,<val2>,...")
            print("  SELECT * FROM <table_name> [WHERE key = <value>] [SHOW_HASH]")
            print("  DELETE FROM <table_name> WHERE key = <value>    (未実装)")
            print("  FLASH")
            print("  EXIT")
            print("========================================")
        
        print("===== DBMS Interactive Shell =====")
        print("利用可能なコマンド: (HELP または ? でヘルプ表示)")
        
        while True:
            try:
                cmd = input("db> ").strip()
            except EOFError:
                break
            if not cmd:
                continue
            
            cmd_lower = cmd.lower()
            
            if cmd_lower in ["exit", "quit"]:
                print("シェルを終了します。フラッシュしてデータ永続化...")
                self.flush()
                break
            
            elif cmd_lower in ["help", "?"]:
                print_help()
            
            # 隠しコマンド：dbmsfan（ヘルプには表示しない）
            elif cmd_lower == "dbmsfan":
                print("★ DBMS FAN ★")
                print("あなたはDBMSオタク！ディスクI/O、バッファ管理、B+Treeの神髄に魅了されし者よ、情熱を燃やせ！")
            
            # 隠しコマンド：show btree ascii <table_name>
            elif cmd_lower.startswith("show btree ascii"):
                parts = cmd.split()
                if len(parts) < 4:
                    print("Usage: SHOW BTREE ASCII <table_name>")
                    continue
                table_name = parts[3]
                try:
                    self.print_btree_ascii(table_name)
                except Exception as e:
                    print(f"エラー: {e}")
            
            # 隠しコマンド：show btree <table_name>
            elif cmd_lower.startswith("show btree"):
                parts = cmd.split()
                if len(parts) < 3:
                    print("Usage: SHOW BTREE <table_name>")
                    continue
                table_name = parts[2]
                try:
                    self.show_btree(table_name)
                except Exception as e:
                    print(f"エラー: {e}")
            
            # 隠しコマンド：show page <page_id>
            elif cmd_lower.startswith("show page"):
                parts = cmd.split()
                if len(parts) < 3:
                    print("Usage: SHOW PAGE <page_id>")
                    continue
                page_id_str = parts[2]
                self.show_page(page_id_str)
            
            elif cmd_lower.startswith("create table"):
                parts = cmd.split()
                if len(parts) < 3:
                    print("Usage: CREATE TABLE <table_name>")
                    continue
                table_name = parts[2]
                try:
                    msg = self.create_table(table_name)
                    print(msg)
                except Exception as e:
                    print(f"エラー: {e}")
            
            elif cmd_lower.startswith("drop table"):
                parts = cmd.split()
                if len(parts) < 3:
                    print("Usage: DROP TABLE <table_name>")
                    continue
                table_name = parts[2]
                try:
                    msg = self.drop_table(table_name)
                    print(msg)
                except Exception as e:
                    print(f"エラー: {e}")
            
            elif cmd_lower.startswith("show tables"):
                table_list = self.list_tables()
                if table_list:
                    print("----- テーブル一覧 -----")
                    for t in table_list:
                        print(t)
                    print("------------------------")
                else:
                    print("テーブルが存在しません。")
            
            elif cmd_lower.startswith("insert into"):
                if "values" not in cmd_lower:
                    print("Usage: INSERT INTO <table_name> VALUES <val1>,<val2>,...")
                    continue
                tokens = cmd.split("values", 1)
                if len(tokens) < 2:
                    print("Usage: INSERT INTO <table_name> VALUES <val1>,<val2>,...")
                    continue
                first_part = tokens[0].strip()  # "INSERT INTO <table_name>"
                values_part = tokens[1].strip()  # "val1,val2,..."
                parts = first_part.split()
                if len(parts) < 3:
                    print("テーブル名が必要です。")
                    continue
                table_name = parts[2]
                value_tokens = [v.strip() for v in values_part.split(",") if v.strip()]
                try:
                    msg = self.insert_into(table_name, value_tokens)
                    print(msg)
                except Exception as e:
                    print(f"エラー: {e}")
            
            elif cmd_lower.startswith("select"):
                tokens = cmd.split()
                if len(tokens) < 4:
                    print("Usage: SELECT * FROM <table_name> [WHERE key = <value>] [SHOW_HASH]")
                    continue
                if tokens[1] != "*" or tokens[2].lower() != "from":
                    print("Unsupported SELECT format.")
                    continue
                table_name = tokens[3]
                where_key = None
                show_hash = False
                if "where" in cmd_lower:
                    idx = cmd_lower.find("where")
                    where_clause = cmd_lower[idx:].split()
                    if len(where_clause) >= 4 and where_clause[1] == "key" and where_clause[2] == "=":
                        where_key = where_clause[3]
                if "show_hash" in cmd_lower:
                    show_hash = True
                try:
                    records = self.select_from(table_name, where_key, show_hash)
                    if not records:
                        print("レコードが見つかりません。")
                    else:
                        print(f"----- テーブル: {table_name} -----")
                        for rec in records:
                            print(rec)
                        print("------------------------------")
                except Exception as e:
                    print(f"エラー: {e}")
            
            elif cmd_lower.startswith("delete from"):
                print("DELETE 機能は現在未実装です。")
            
            elif cmd_lower.startswith("flash"):
                try:
                    self.flush()
                    print("すべてのダーティページがディスクにフラッシュされました。")
                except Exception as e:
                    print(f"エラー: {e}")
            
            else:
                print("不明なコマンドです。利用可能なコマンドは 'help' または '?' で確認してください。")
        
        print("シェルを終了しました。")

def main():
    engine = QueryEngine(heap_file="mydb.rly", pool_size=10)
    engine.run_shell()

if __name__ == "__main__":
    main()
