# KVBench

cloned from [tidwall/kvbench](https://github.com/tidwall/kvbench)

KVBench is a Redis server clone backed by a few different Go databases. 

It's intended to be used with the `redis-benchmark` command to test the performance of various Go databases.
It has support for redis pipelining.

this cloned version adds more kv databases and automatic scripts.

Features:

- Databases
  - [badger](https://github.com/dgraph-io/badger)
  - [BboltDB](https://github.com/etcd-io/bbolt)
  - [BoltDB](https://github.com/boltdb/bolt)
  - [buntdb](https://github.com/tidwall/buntdb)
  - [LevelDB](https://github.com/syndtr/goleveldb)
  - [cznic/kv](https://github.com/cznic/kv)
  - map (in-memory) with [AOF persistence](https://redis.io/topics/persistence)
  - btree (in-memory) with [AOF persistence](https://redis.io/topics/persistence)
- Option to disable fsync
- Compatible with Redis clients

## Benchmarks

Test concurrency cases:

- set: use 40 goroutines to write
- get: use 40 goroutines to read
- mixed: use one goroutine to write and use 40 goroutines to read
- del: use 40 goroutines to delete

Use 10000 items to test. Size of value of items is 256 bytes and size of key of items is 9 bytes.

### fsync disabled

#### throughouts

![set](cmd/cli/benchmark/nofsync-set-throughputs.png)
![get](cmd/cli/benchmark/nofsync-get-throughputs.png)
![mixed](cmd/cli/benchmark/nofsync-mixed-throughputs.png)
![del](cmd/cli/benchmark/nofsync-del-throughputs.png)

#### time

![set](cmd/cli/benchmark/nofsync-set-time.png)
![get](cmd/cli/benchmark/nofsync-get-time.png)
![mixed](cmd/cli/benchmark/nofsync-mixed-time.png)
![del](cmd/cli/benchmark/nofsync-del-time.png)

### fsync enabled

#### throughouts

![set](cmd/cli/benchmark/fsync-set-throughputs.png)
![get](cmd/cli/benchmark/fsync-get-throughputs.png)
![mixed](cmd/cli/benchmark/fsync-mixed-throughputs.png)
![del](cmd/cli/benchmark/fsync-del-throughputs.png)

#### time

![set](cmd/cli/benchmark/fsync-set-time.png)
![get](cmd/cli/benchmark/fsync-get-time.png)
![mixed](cmd/cli/benchmark/fsync-mixed-time.png)
![del](cmd/cli/benchmark/fsync-del-time.png)