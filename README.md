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

### Socket
```shell
nc 127.0.0.1 8080
# use
GET key731 # get value for key
SET key1001=value1001 # sen new pair
GET_LEN # cache size
GET_ALL # print all
```

### Output
```shell
Write Benchmark Complete: 1000 requests, Total Time: 705.879621ms, Avg Time per Request: 705.879µs
```

### Python
```shell
python3 -m venv venv
source venv/bin/activate
python3 -m pip install pyarrow
python3 -m pip install pandas
python3 arrow.py
deactivate
```

### Arrow IPC to Parquet
```rust
use std::fs::File;
use std::sync::Arc;

use arrow::ipc::reader::FileReader as ArrowIPCReader;
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Step 1: Read Arrow IPC file
    let ipc_file = File::open("data.arrow")?;
    let mut ipc_reader = ArrowIPCReader::try_new(ipc_file)?;
    
    // Collect all RecordBatches from the IPC file
    let mut record_batches = vec![];
    while let Some(batch) = ipc_reader.next()? {
        record_batches.push(batch);
    }

    // Ensure at least one RecordBatch is available
    if record_batches.is_empty() {
        return Err("No data found in the Arrow IPC file.".into());
    }

    // Combine RecordBatches into a single schema if needed
    let schema = record_batches[0].schema();

    // Step 2: Write to Parquet
    let parquet_file = File::create("output.parquet")?;
    let writer_properties = WriterProperties::builder().build();
    let mut parquet_writer = ArrowWriter::try_new(parquet_file, schema, Some(writer_properties))?;

    // Write each RecordBatch to the Parquet file
    for batch in record_batches {
        parquet_writer.write(&batch)?;
    }

    // Close the writer to flush data to disk
    parquet_writer.close()?;

    println!("Converted Arrow IPC to Parquet successfully.");
    Ok(())
}
```
