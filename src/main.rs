use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::net::{TcpListener, TcpStream, UdpSocket};
use tokio::sync::Mutex;
use tokio::task;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use socket2::{Socket, Domain, Type};
use log::{error, trace, debug, info, warn};
use log4rs;
use arrow::array::StringArray;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use arrow::ipc::writer::FileWriter;
use std::fs::File;

type SharedCache = Arc<Mutex<HashMap<String, String>>>;
type PeerList = Arc<Mutex<HashSet<String>>>;

const DISCOVERY_PORT: u16 = 9000;

async fn write_cache_to_arrow(cache: SharedCache, file_path: &str) -> Result<(), Box<dyn std::error::Error>> {
    // Lock the cache and extract key-value pairs
    let cache_snapshot = cache.lock().await;
    let keys: Vec<&String> = cache_snapshot.keys().collect();
    let values: Vec<&String> = cache_snapshot.values().collect();

    // Create Arrow arrays for keys and values
    let keys_array = StringArray::from(keys.iter().map(|s| s.as_str()).collect::<Vec<&str>>());
    let values_array = StringArray::from(values.iter().map(|s| s.as_str()).collect::<Vec<&str>>());

    // Define Arrow schema
    let schema = Schema::new(vec![
        Field::new("key", DataType::Utf8, false),
        Field::new("value", DataType::Utf8, false),
    ]);

    // Create a RecordBatch
    let record_batch = RecordBatch::try_new(
        Arc::new(schema),
        vec![Arc::new(keys_array), Arc::new(values_array)],
    )?;

    // Write to Arrow file
    let file = File::create(file_path)?;
    let mut writer = FileWriter::try_new(file, &record_batch.schema())?;
    writer.write(&record_batch)?;
    writer.finish()?;

    Ok(())
}

async fn save_cache_periodically(cache: SharedCache, file_path: String) {
    loop {
        if let Err(e) = write_cache_to_arrow(Arc::clone(&cache), &file_path).await {
            error!("Failed to save cache to Arrow file: {}", e);
        } else {
            debug!("Cache saved to Arrow file: {}", file_path);
        }

        // Sleep for 10 seconds before saving again
        tokio::time::sleep(tokio::time::Duration::from_secs(10)).await;
    }
}

async fn discovery_service(peers: PeerList) {
    let socket = Socket::new(Domain::IPV4, Type::DGRAM, None).unwrap();
    socket.set_reuse_address(true).unwrap();
    #[cfg(unix)]
    socket.set_reuse_port(true).unwrap();
    socket.bind(&"0.0.0.0:9000".parse::<std::net::SocketAddr>().unwrap().into()).unwrap();

    let socket = UdpSocket::from_std(socket.into()).unwrap();
    debug!("Discovery service listening on UDP port {}", DISCOVERY_PORT);

    let mut buf = [0u8; 1024];
    loop {
        if let Ok((len, _)) = socket.recv_from(&mut buf).await {
            let message = String::from_utf8_lossy(&buf[..len]);
            if message.starts_with("ANNOUNCE") {
                // Add the peer to the peer list
                let peer_addr = message[9..].trim().to_string();
                debug!("Discovered peer: {}", peer_addr);
                peers.lock().await.insert(peer_addr);
            }
            check_for_expired_peers(peers.clone()).await;
        }
    }
}

async fn check_for_expired_peers(peers: PeerList) {
    let mut peers = peers.lock().await;
    let mut expired_peers = Vec::new();
    for peer in peers.iter() {
        if TcpStream::connect(peer).await.is_err() {
            expired_peers.push(peer.clone());
        }
    }

    for peer in expired_peers {
        peers.remove(&peer);
        warn!("Removed expired peer: {}", peer);
    }
}

// async fn check_for_expired_peers(peers: PeerList) {
//     let mut peers = peers.lock().await;
//     let mut expired_peers = Vec::new();
//     for peer in peers.iter() {
//         if let Err(_) = TcpStream::connect(peer).await {
//             expired_peers.push(peer.clone());
//         }
//     }

//     for peer in expired_peers {
//         peers.remove(&peer);
//         warn!("Removed expired peer: {}", peer);
//     }
// }

async fn announce_self(node_port: u16) { //peers: PeerList,
    let socket = UdpSocket::bind(("0.0.0.0", 0)).await.unwrap();
    socket.set_broadcast(true).unwrap();

    let broadcast_address = "255.255.255.255:9000";

    loop {
        let message = format!("ANNOUNCE 127.0.0.1:{}", node_port);
        debug!("Broadcasting: {}", message);
        if let Err(e) = socket.send_to(message.as_bytes(), broadcast_address).await {
            error!("Failed to broadcast: {}", e);
        }

        // {
        //     let peers_snapshot = peers.lock().await;
        //     trace!("Known peers: {:?}", peers_snapshot);
        // }

        tokio::time::sleep(tokio::time::Duration::from_secs(10)).await;
    }
}

async fn handle_connection(mut socket: TcpStream, cache: SharedCache, peers: PeerList) {
    let mut buffer = [0; 1024];

    match socket.read(&mut buffer).await {
        Ok(bytes_read) if bytes_read > 0 => {
            let request = String::from_utf8_lossy(&buffer[..bytes_read]);
            debug!("Received: {}", request);

            let response = if request.starts_with("GET") {
                if request.starts_with("GET_ALL") {
                    debug!("Processing GET_ALL");

                    let cache = cache.lock().await;
                    let all_pairs: String = cache
                        .iter()
                        .map(|(key, value)| format!("{}={}", key, value))
                        .collect::<Vec<_>>()
                        .join("\n");

                    all_pairs
                } else if request.starts_with("GET_LEN") {
                    debug!("Processing GET_LEN");

                    let cache = cache.lock().await;
                    cache.len().to_string()
                } else {
                    let key = request[4..].trim();
                    debug!("Processing GET for key: {}", key);

                    let cache = cache.lock().await;
                    cache.get(key).cloned().unwrap_or_else(|| "Not Found".to_string())
                }
            } else if request.starts_with("SET") {
                // Local SET request
                let parts: Vec<&str> = request[4..].split('=').collect();
                if parts.len() == 2 {
                    let key = parts[0].trim().to_string();
                    let value = parts[1].trim().to_string();
                    debug!("Processing local SET for key: {}, value: {}", key, value);

                    // Update local cache
                    {
                        let mut cache = cache.lock().await;
                        cache.insert(key.clone(), value.clone());
                    }

                    // Broadcast to peers
                    let peers_clone = Arc::clone(&peers);
                    tokio::spawn(async move {
                        broadcast_set(peers_clone, key, value).await;
                    });

                    format!("OK: SET successful")
                } else {
                    "Invalid SET command".to_string()
                }
            } else if request.starts_with("BROADCAST") {
                // Received broadcasted SET
                let parts: Vec<&str> = request[10..].split('=').collect();
                if parts.len() == 2 {
                    let key = parts[0].trim().to_string();
                    let value = parts[1].trim().to_string();
                    debug!("Processing BROADCAST for key: {}, value: {}", key, value);

                    // Update local cache (no re-broadcast)
                    let mut cache = cache.lock().await;
                    cache.insert(key, value);

                    format!("OK: BROADCAST applied")
                } else {
                    "Invalid BROADCAST command".to_string()
                }
            } else {
                "Unknown command".to_string()
            };

            debug!("Sending response: {}", response);
            if let Err(e) = socket.write_all(response.as_bytes()).await {
                error!("Failed to send response: {}", e);
            }
        }
        Ok(_) => debug!("Connection closed by client."),
        Err(e) => error!("Failed to read from socket: {}", e),
    }
}

async fn node_listener(peers: PeerList, cache: SharedCache, node_port: u16) {
    let listener = TcpListener::bind(("0.0.0.0", node_port)).await.unwrap();
    info!("Node listening on TCP port {}", node_port);

    loop {
        if let Ok((socket, addr)) = listener.accept().await {
            debug!("New connection from {}", addr);

            let cache = Arc::clone(&cache);
            let peers = Arc::clone(&peers);
            task::spawn(async move {
                handle_connection(socket, cache, peers).await;
            });
        }
    }
}

async fn broadcast_set(peers: PeerList, key: String, value: String) {
    let peers_snapshot = peers.lock().await.clone(); // Clone to avoid holding the lock for too long
    for peer in peers_snapshot.iter() {
        if let Ok(mut stream) = TcpStream::connect(peer).await {
            let message = format!("BROADCAST {}={}\n", key, value); // Use BROADCAST prefix
            if let Err(e) = stream.write_all(message.as_bytes()).await {
                error!("Failed to send BROADCAST to {}: {}", peer, e);
            } else {
                debug!("Broadcasted BROADCAST {}={} to {}", key, value, peer);
            }
        } else {
            warn!("Failed to connect to peer: {}", peer);
        }
    }
}

#[tokio::main]
async fn main() {
    log4rs::init_file("log4rs.yaml", Default::default()).unwrap();

    // Shared cache and peer list
    let cache: SharedCache = Arc::new(Mutex::new(HashMap::new()));
    let peers: PeerList = Arc::new(Mutex::new(HashSet::new()));

    // Assign a unique TCP port for this node
    let node_port = std::env::args().nth(1).unwrap_or("8080".to_string()).parse::<u16>().unwrap();

    // Start the discovery service
    let peers_clone = Arc::clone(&peers);
    tokio::spawn(discovery_service(peers_clone));

    // Announce this node to the network
    //let peers_clone = Arc::clone(&peers);
    tokio::spawn(announce_self(node_port)); //peers_clone

    // Periodically print current peers
    let peers_clone = Arc::clone(&peers);
    tokio::spawn(async move {
        loop {
            let peers_snapshot = peers_clone.lock().await;
            trace!("Current peers: {:?}", peers_snapshot);
            tokio::time::sleep(tokio::time::Duration::from_secs(10)).await;
        }
    });

    // File path to save the Arrow file
    let file_path = format!("node_{}_cache.arrow", node_port);

    // Periodically save cache to Arrow file
    let cache_clone = Arc::clone(&cache);
    tokio::spawn(save_cache_periodically(cache_clone, file_path));

    // Start the TCP listener for peer-to-peer communication
    node_listener(peers, cache, node_port).await;
}
