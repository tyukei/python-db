import threading
from main import load_schemas, create_table, insert, select, format_select_result

def insert_data(thread_id):
    for i in range(10):
        command = f"INSERT INTO users (id, name, age) VALUES ({thread_id * 10 + i}, 'User{thread_id * 10 + i}', {20 + i})"
        insert(command)

def main():
    load_schemas()
    create_table("CREATE TABLE users (id int, name string, age int)")
    
    threads = []
    for i in range(5):  # 5つのスレッドを作成
        thread = threading.Thread(target=insert_data, args=(i,))
        threads.append(thread)
        thread.start()
    
    for thread in threads:
        thread.join()
    
    result = select("SELECT * FROM users")
    columns = ['id', 'name', 'age']
    format_select_result(result, columns)

if __name__ == "__main__":
    main()
