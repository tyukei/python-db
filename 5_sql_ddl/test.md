```
db > CREATE TABLE users (id int, name string, age int)
Table users created with columns {'id': 'int', 'name': 'string', 'age': 'int'}
db > INSERT INTO users (id, name, age) VALUES (1, 'Alice', 30)
db > INSERT INTO users (id, name, age) VALUES (2, 'Bob', 25)
db > SELECT * FROM users
id, name, age
1, 'Alice', 30
2, 'Bob', 25
db > ALTER TABLE users ADD COLUMN email string
Column email added to table users.
db > SELECT * FROM users
id, name, age, email
1, 'Alice', 30, None
2, 'Bob', 25, None
db > ALTER TABLE users DROP COLUMN age
Column age dropped from table users.
db > SELECT * FROM users
id, name, email
1, 'Alice', None
2, 'Bob', None
db > DROP TABLE users
Table users dropped.
db > SELECT * FROM users
Table users does not exist.
db > exit
```