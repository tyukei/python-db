# DEMO

ターミナル実行結果

```
(.venv)$ python main.py      
Inserting data...
Inserting key: 6, value: world
Inserting key: 3, value: hello
Inserting key: 8, value: !
Inserting key: 4, value: ,
Flushing buffers to disk...
Flushing page 0 to disk
Flushing page 1 to disk
Searching data...
Key: 3, Value: hello
Key: 8, Value: !
```

simple.rlyの結果
```
(.venv)$ python check_file.py
Page 0: b'\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00'
Page 1: b'\x00\x00\x00\x00\x00\x00\x00\x04\x00\x00\x00F\x80\x04\x95;\x00\x00\x00\x00\x00\x00\x00\x8c\x05btree\x94\x8c\x04Pair\x94\x93\x94)\x81\x94}\x94(\x8c\x03key\x94C\x08\x00\x00\x00\x00\x00\x00\x00\x03\x94\x8c'
Page 2: b''
Page 3: b''
```