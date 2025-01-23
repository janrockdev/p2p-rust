use std::fs::File;
use std::io::{Read, Write};
use std::net::TcpStream;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use arrow::ipc::reader::FileReader;
use arrow::array::Array;
use std::collections::HashMap;

type SharedCache = Arc<Mutex<HashMap<String, String>>>;

fn send_request(node: &str, request: &str) -> Option<String> {
    match TcpStream::connect(node) {
        Ok(mut stream) => {
            stream.write_all(request.as_bytes()).unwrap();
            let mut buffer = [0; 1024];
            let bytes_read = stream.read(&mut buffer).unwrap();
            Some(String::from_utf8_lossy(&buffer[..bytes_read]).to_string())
        }
        Err(e) => {
            eprintln!("Failed to connect to {}: {}", node, e);
            None
        }
    }
}

fn get_from_arrow(file_path: &str, key: &str) -> Option<String> {
    // Open the Arrow file
    let file = File::open(file_path).ok()?;
    let reader = FileReader::try_new(file, None).ok()?; // Add `None` for the second argument

    for batch in reader {
        let batch = batch.ok()?;
        if let Some(key_array) = batch.column(0).as_any().downcast_ref::<arrow::array::StringArray>()
        {
            if let Some(value_array) = batch.column(1).as_any().downcast_ref::<arrow::array::StringArray>() {
                for i in 0..key_array.len() {
                    if key_array.value(i) == key {
                        return Some(value_array.value(i).to_string());
                    }
                }
            }
        }
    }
    None
}


fn benchmark_write(cache: SharedCache, write_node: &str, num_requests: usize) {
    let mut total_time = Duration::ZERO;

    for i in 0..num_requests {
        let key = format!("key{}", i);
        let value = format!("value{}", i);

        // Update local cache
        {
            let mut cache = cache.lock().unwrap();
            cache.insert(key.clone(), value.clone());
        }

        // Send SET request to write node
        let request = format!("SET {}={}\n", key, value);
        let start = Instant::now();
        if let Some(response) = send_request(write_node, &request) {
            println!("Write Response: {}", response);
        }
        total_time += start.elapsed();
    }

    println!(
        "Write Benchmark Complete: {} requests, Total Time: {:?}, Avg Time per Request: {:?}",
        num_requests,
        total_time,
        total_time / num_requests as u32
    );
}

fn benchmark_read(cache: SharedCache, _: &str, file_path: &str, num_requests: usize) {
    let mut total_time = Duration::ZERO;

    for i in 0..num_requests {
        let key = format!("key{}", i);

        // Attempt to get from cache first
        let start = Instant::now();
        let value = {
            let cache = cache.lock().unwrap();
            cache.get(&key).cloned()
        };

        let value = match value {
            Some(v) => Some(v),
            None => {
                // If not in cache, attempt to get from Arrow
                get_from_arrow(file_path, &key)
            }
        };

        if let Some(val) = value {
            println!("Read Response: key={}, value={}", key, val);
        } else {
            println!("Read Response: key={} not found", key);
        }
        total_time += start.elapsed();
    }

    println!(
        "Read Benchmark Complete: {} requests, Total Time: {:?}, Avg Time per Request: {:?}",
        num_requests,
        total_time,
        total_time / num_requests as u32
    );
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 6 {
        eprintln!(
            "Usage: {} <write_node> <read_node> <file_path> <mode: write|read|both> <num_requests>",
            args[0]
        );
        return;
    }

    let write_node = &args[1];
    let read_node = &args[2];
    let file_path = &args[3];
    let mode = &args[4];
    let num_requests: usize = args[5].parse().unwrap_or(100);

    let cache: SharedCache = Arc::new(Mutex::new(HashMap::new()));

    match mode.as_str() {
        "write" => benchmark_write(cache, write_node, num_requests),
        "read" => benchmark_read(cache, read_node, file_path, num_requests),
        "both" => {
            benchmark_write(Arc::clone(&cache), write_node, num_requests);
            benchmark_read(Arc::clone(&cache), read_node, file_path, num_requests);
        }
        _ => eprintln!("Invalid mode. Use 'write', 'read', or 'both'."),
    }
}
