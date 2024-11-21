import json
import re
import os
import threading

# スキーマを保存するための辞書
schemas = {}

# トランザクション管理用の変数
in_transaction = False
transaction_buffer = {}
transaction_lock = threading.Lock()

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

def save_table_data(table_name, data):
    with transaction_lock:
        if in_transaction:
            transaction_buffer[table_name] = data
        else:
            with open(f'{table_name}.json', 'w') as f:
                json.dump(data, f)

def load_table_data(table_name):
    with transaction_lock:
        if in_transaction and table_name in transaction_buffer:
            return transaction_buffer[table_name]
        try:
            with open(f'{table_name}.json', 'r') as f:
                return json.load(f)
        except FileNotFoundError:
            return []

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
        print(', '.join(str(row.get(col, '')) for col in columns))

def drop_table(command):
    match = re.match(r'drop table (\w+)', command, re.IGNORECASE)
    if not match:
        print("Invalid DROP TABLE command.")
        return
    table_name = match.group(1)
    if table_name not in schemas:
        print(f"Table {table_name} does not exist.")
        return
    del schemas[table_name]
    save_schemas()
    try:
        os.remove(f'{table_name}.json')
        print(f"Table {table_name} dropped.")
    except FileNotFoundError:
        print(f"Data file for table {table_name} not found.")

def alter_table(command):
    match_add = re.match(r'alter table (\w+) add column (\w+) (\w+)', command, re.IGNORECASE)
    match_drop = re.match(r'alter table (\w+) drop column (\w+)', command, re.IGNORECASE)
    
    if match_add:
        table_name = match_add.group(1)
        column_name = match_add.group(2)
        column_type = match_add.group(3)
        if table_name not in schemas:
            print(f"Table {table_name} does not exist.")
            return
        schemas[table_name][column_name] = column_type
        save_schemas()
        # 既存のデータに新しいカラムを追加
        table = load_table_data(table_name)
        for row in table:
            row[column_name] = None
        save_table_data(table_name, table)
        print(f"Column {column_name} added to table {table_name}.")
    elif match_drop:
        table_name = match_drop.group(1)
        column_name = match_drop.group(2)
        if table_name not in schemas:
            print(f"Table {table_name} does not exist.")
            return
        if column_name not in schemas[table_name]:
            print(f"Column {column_name} does not exist in table {table_name}.")
            return
        del schemas[table_name][column_name]
        save_schemas()
        # 既存のデータからカラムを削除
        table = load_table_data(table_name)
        for row in table:
            if column_name in row:
                del row[column_name]
        save_table_data(table_name, table)
        print(f"Column {column_name} dropped from table {table_name}.")
    else:
        print("Invalid ALTER TABLE command.")

def update(command):
    match = re.match(r'update (\w+) set (.+) where (.+)', command, re.IGNORECASE)
    if not match:
        print("Invalid UPDATE command.")
        return
    table_name = match.group(1)
    set_clause = match.group(2)
    where_clause = match.group(3)
    if table_name not in schemas:
        print(f"Table {table_name} does not exist.")
        return
    table = load_table_data(table_name)
    set_conditions = {cond.split('=')[0].strip(): cond.split('=')[1].strip() for cond in set_clause.split(',')}
    where_conditions = {cond.split('=')[0].strip(): cond.split('=')[1].strip() for cond in where_clause.split('and')}
    for row in table:
        if all(row.get(k) == v for k, v in where_conditions.items()):
            for k, v in set_conditions.items():
                row[k] = v
    save_table_data(table_name, table)

def delete(command):
    match = re.match(r'delete from (\w+) where (.+)', command, re.IGNORECASE)
    if not match:
        print("Invalid DELETE command.")
        return
    table_name = match.group(1)
    where_clause = match.group(2)
    if table_name not in schemas:
        print(f"Table {table_name} does not exist.")
        return
    table = load_table_data(table_name)
    where_conditions = {cond.split('=')[0].strip(): cond.split('=')[1].strip() for cond in where_clause.split('and')}
    table = [row for row in table if not all(row.get(k) == v for k, v in where_conditions.items())]
    save_table_data(table_name, table)

def begin_transaction():
    global in_transaction, transaction_buffer
    with transaction_lock:
        if in_transaction:
            print("Transaction already in progress.")
            return
        in_transaction = True
        transaction_buffer = {}
        print("Transaction started.")

def commit():
    global in_transaction, transaction_buffer
    with transaction_lock:
        if not in_transaction:
            print("No transaction in progress.")
            return
        for table_name, data in transaction_buffer.items():
            with open(f'{table_name}.json', 'w') as f:
                json.dump(data, f)
        in_transaction = False
        transaction_buffer = {}
        print("Transaction committed.")

def rollback():
    global in_transaction, transaction_buffer
    with transaction_lock:
        if not in_transaction:
            print("No transaction in progress.")
            return
        in_transaction = False
        transaction_buffer = {}
        print("Transaction rolled back.")

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
        elif command.lower().startswith("update"):
            update(command)
        elif command.lower().startswith("delete from"):
            delete(command)
        elif command.lower().startswith("drop table"):
            drop_table(command)
        elif command.lower().startswith("alter table"):
            alter_table(command)
        elif command.lower() == "begin transaction":
            begin_transaction()
        elif command.lower() == "commit":
            commit()
        elif command.lower() == "rollback":
            rollback()
        elif command.lower() == "exit":
            break

if __name__ == "__main__":
    main()
