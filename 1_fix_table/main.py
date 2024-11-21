# テーブルを固定
# CREATE TABLE users (
#     id INT PRIMARY KEY,
#     name TEXT,
#     age INT
# );

import json

def save_to_file(data, filename='database.json'):
    with open(filename, 'w') as f:
        json.dump(data, f)

def load_from_file(filename='database.json'):
    try:
        with open(filename, 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        return []
    
def insert(data, table, values):
    table.append(values)
    save_to_file(table)

def select(table, conditions=None):
    if conditions is None:
        return table
    else:
        return [row for row in table if all(row[k] == v for k, v in conditions.items())]

def main():
    table = load_from_file()
    while True:
        command = input("db > ").strip().lower()
        if command.startswith("insert"):
            _, values = command.split(" ", 1)
            values = json.loads(values)
            insert(table, table, values)
        elif command.startswith("select"):
            print(select(table))
        elif command == "exit":
            break

if __name__ == "__main__":
    main()