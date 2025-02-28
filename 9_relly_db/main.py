from query import QueryEngine

def main():
    engine = QueryEngine(heap_file="mydb.rly", pool_size=10)
    # もし必要なら、以下のようにコマンドラインオプションで実行モードを選択できるようにしてもよい
    engine.run_shell()  # QueryEngine に run_shell() メソッドを追加して対話型シェルを実装しても良いです

if __name__ == "__main__":
    main()
