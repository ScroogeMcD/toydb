## ToyDB : an embedded Key,Value store

ToyDB is an embedded key-value store, which uses disk to store values. It uses an in-memory memtable to store the DB index, which indexes into a SortedStringTable (SSTable) stored on disk. I have taken ideas from RocksDB for this implementation.   


### Concepts
#### 1. Data storage.  
Every write (of a Key,value pair) is done to an in-memory **MemTable** (a Red-Black tree). This MemTables stores <Key,Value> pairs in memory until a certain threshold is reached.
Once this threshold is reached, this memtable is serialized to a file on disk. The entries in the MemTable are stored in the increasing order (sorted order) of the keys in the file.
Hence the name **SortedStringTable**(SSTable).    
Each entry in the file on disk is of the following format 

2 bytes (payload length)  | (k + 1 + v) payload bytes 
--- | ------

The first two bytes represent the length **'N'** of the payload in bytes.   
The next **N = (k+1+v)** bytes represent **k** bytes of the **key**, **1** byte for the **separator** and **v** bytes for the **value**.   

All the keys in the memtable are serialized and written to disk - in sorted order of key - in the byte format mentioned above. After the memtable is written on disk,
a new empty memtable is created where the next writes go to, which eventually gets serialized written to disk on reaching the threshold, and this process continues.

#### 2. Index
#### 3. Crash Recovery
