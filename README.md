# Peer-2-Peer Network with Apache Arrow in Rust

This project is built to test the Rust peer-to-peer library `libp2p`. It demonstrates how to create a simple peer-to-peer network where multiple nodes can communicate with each other using shared in-memory cache. The project includes examples of setting up nodes, connecting them, and running benchmarks to measure performance.

Each node has in-memory cache and Apache Arrow persistent cache.

### Run
```shell
# open terminal #1 (node)
make run
# open terminal #2 (node)
make run
# ...
# open terminal #3 (benchmark) /write/read/both
cargo run --bin client 127.0.0.1:8080 127.0.0.1:8081 node_8080_cache.arrow write 1000
cargo run --bin client 127.0.0.1:8080 127.0.0.1:8081 node_8080_cache.arrow read 1000
```

### Output
```shell
Write Benchmark Complete: 1000 requests, Total Time: 705.879621ms, Avg Time per Request: 705.879µs
```