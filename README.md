# caskdb

A small, embeddable log-structured key/value store for Go, modelled on
[Bitcask](https://riak.com/assets/bitcask-intro.pdf).

Writes are appended to data files, an in-memory hash index maps each key to
the position of its latest value, and every record is CRC-checked.

```go
db, err := caskdb.Open("./data", nil)
if err != nil { log.Fatal(err) }
defer db.Close()

db.Put([]byte("greeting"), []byte("hello"))
v, _ := db.Get([]byte("greeting"))
```

Work in progress. Run the tests with `go test -race ./...`.

MIT licensed.
