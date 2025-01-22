# Peer-2-Peer Network in Rust

This project is built to test the Rust peer-to-peer library `libp2p`. It demonstrates how to create a simple peer-to-peer network where multiple nodes can communicate with each other using shared in-memory cache. The project includes examples of setting up nodes, connecting them, and running benchmarks to measure performance.

### Run
```shell
# open terminal #1 (node)
make run
# open terminal #2 (node)
make run
# ...
# open terminal #3 (benchmark)
make client # <-------------------- AND PRESS ENTER
```

### Output
```shell
Total PUT operations: 1000
Average PUT time: 202.06 µs
```