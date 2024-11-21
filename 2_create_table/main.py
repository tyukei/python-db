import json

# スキーマを保存するための辞書
schemas = {}

def save_schemas(filename='schemas.json'):
    with open(filename, 'w') as f:
        json.dump(schemas, f)

def load_schemas(filename='schemas.json'):
    global schemas
    try:
        with open(filename, 'r') as f:
            schemas = json.load(f)
    except FileNotFoundError:
        schemas = {}

def create_table(command):
    global schemas
    command_parts = command.strip().split()
    if len(command_parts) < 4:
        print("Invalid CREATE TABLE command.")
        return
    table_name = command_parts[2]
    columns_def = command[command.index('(') + 1:command.index(')')].split(',')
    columns = [col.strip().split() for col in columns_def]
    if any(len(col) != 2 for col in columns):
        print("Invalid column definition.")
        return
    schemas[table_name] = {col[0]: col[1] for col in columns}
    save_schemas()
    print(f"Table {table_name} created with columns {schemas[table_name]}")

def save_table_data(table_name, data):
    with open(f'{table_name}.json', 'w') as f:
        json.dump(data, f)

def load_table_data(table_name):
    try:
        with open(f'{table_name}.json', 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        return []

def insert(table_name, values):
    if table_name not in schemas:
        print(f"Table {table_name} does not exist.")
        return
    table = load_table_data(table_name)
    schema = schemas[table_name]
    # スキーマに基づいてデータを検証
    for column in schema:
        if column not in values:
            print(f"Missing value for column {column}")
            return
    table.append(values)
    save_table_data(table_name, table)

def select(table_name, conditions=None):
    if table_name not in schemas:
        print(f"Table {table_name} does not exist.")
        return []
    table = load_table_data(table_name)
    if conditions is None:
        return table
    else:
        return [row for row in table if all(row.get(k) == v for k, v in conditions.items())]

def main():
    load_schemas()
    while True:
        command = input("db > ").strip().lower()
        if command.startswith("create table"):
            create_table(command)
        elif command.startswith("insert"):
            _, table_name, values = command.split(" ", 2)
            values = json.loads(values)
            insert(table_name, values)
        elif command.startswith("select"):
            _, table_name = command.split(" ", 1)
            print(select(table_name))
        elif command == "exit":
            break

if __name__ == "__main__":
    main()
