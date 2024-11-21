import json
import re

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
    match = re.match(r'create table (\w+) \((.+)\)', command, re.IGNORECASE)
    if not match:
        print("Invalid CREATE TABLE command.")
        return
    table_name = match.group(1)
    columns_def = match.group(2).split(',')
    columns = [col.strip().split() for col in columns_def]
    if any(len(col) != 2 for col in columns):
        print("Invalid column definition.")
        return
    schemas[table_name] = {col[0]: col[1].lower() for col in columns}
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

def insert(command):
    match = re.match(r'insert into (\w+) \((.+)\) values \((.+)\)', command, re.IGNORECASE)
    if not match:
        print("Invalid INSERT command.")
        return
    table_name = match.group(1)
    columns = match.group(2).split(',')
    values = match.group(3).split(',')
    if table_name not in schemas:
        print(f"Table {table_name} does not exist.")
        return
    schema = schemas[table_name]
    if len(columns) != len(values):
        print("Column count does not match value count.")
        return
    row = {col.strip(): val.strip() for col, val in zip(columns, values)}
    table = load_table_data(table_name)
    table.append(row)
    save_table_data(table_name, table)

def select(command):
    match = re.match(r'select (.+) from (\w+)( where (.+))?', command, re.IGNORECASE)
    if not match:
        print("Invalid SELECT command.")
        return
    columns = match.group(1).split(',')
    table_name = match.group(2)
    conditions = match.group(4)
    if table_name not in schemas:
        print(f"Table {table_name} does not exist.")
        return []
    table = load_table_data(table_name)
    if conditions:
        conditions = {cond.split('=')[0].strip(): cond.split('=')[1].strip() for cond in conditions.split('and')}
        result = [row for row in table if all(row.get(k) == v for k, v in conditions.items())]
    else:
        result = table
    if columns[0].strip() == '*':
        return result
    else:
        return [{col.strip(): row[col.strip()] for col in columns} for row in result]

def format_select_result(result, columns):
    if not result:
        return
    if columns[0].strip() == '*':
        columns = result[0].keys()
    else:
        columns = [col.strip() for col in columns]
    print(', '.join(columns))
    for row in result:
        print(', '.join(str(row[col]) for col in columns))

def main():
    load_schemas()
    while True:
        command = input("db > ").strip()
        if command.lower().startswith("create table"):
            create_table(command)
        elif command.lower().startswith("insert into"):
            insert(command)
        elif command.lower().startswith("select"):
            result = select(command)
            columns = re.match(r'select (.+) from', command, re.IGNORECASE).group(1).split(',')
            format_select_result(result, columns)
        elif command.lower() == "exit":
            break

if __name__ == "__main__":
    main()