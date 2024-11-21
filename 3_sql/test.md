```
db > CREATE TABLE users (id int, name string, age int)
Table users created with columns {'id': 'int', 'name': 'string', 'age': 'int'}
db > CREATE TABLE products (id int, name string, price float)
Table products created with columns {'id': 'int', 'name': 'string', 'price': 'float'}
db > INSERT INTO users (id, name, age) VALUES (1, 'Alice', 30)
db > INSERT INTO users (id, name, age) VALUES (2, 'Bob', 25)
db > INSERT INTO products (id, name, price) VALUES (1, 'Laptop', 999.99)
db > INSERT INTO products (id, name, price) VALUES (2, 'Phone', 499.99)
db > SELECT * FROM users
id, name, age
1, 'Alice', 30
2, 'Bob', 25
db > SELECT name, price FROM products
name, price
'Laptop', 999.99
'Phone', 499.99
db > exit
```