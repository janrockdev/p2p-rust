use std::error::Error;
use tracing_subscriber::EnvFilter;

use futures::stream::StreamExt;
use libp2p::kad::store::RecordStore;
use libp2p::{
    kad,
    kad::{store::MemoryStore, Mode},
    mdns, noise,
    swarm::{NetworkBehaviour, SwarmEvent},
    tcp, yamux,
};
use tokio::{
    io::{self, AsyncBufReadExt},
    select, time,
};

use serde;
use serde::{Deserialize, Serialize};
use std::str;

#[derive(Serialize, Deserialize, Debug)]
struct RecordValue {
    timestamp: u64,
    metadata: String,
    owner: String,
    source: String,
    category: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .init();
    // Custom network behaviour combines Kademlia and mDNS
    #[derive(NetworkBehaviour)]
    struct Behaviour {
        kademlia: kad::Behaviour<MemoryStore>,
        mdns: mdns::tokio::Behaviour,
    }

    let mut swarm = libp2p::SwarmBuilder::with_new_identity()
        .with_tokio()
        .with_tcp(
            tcp::Config::default(),
            noise::Config::new,
            yamux::Config::default,
        )?
        .with_behaviour(|key| {
            Ok(Behaviour {
                kademlia: kad::Behaviour::new(
                    key.public().to_peer_id(),
                    MemoryStore::with_config(
                        key.public().to_peer_id(),
                        kad::store::MemoryStoreConfig {
                            max_records: 1_000_000,
                            max_value_bytes: 65_536_000,
                            max_providers_per_key: 20,
                            max_provided_keys: 1_000_000,
                        },
                    ),
                ),
                mdns: mdns::tokio::Behaviour::new(
                    mdns::Config::default(),
                    key.public().to_peer_id(),
                )?,
            })
        })?
        .build();

    swarm.behaviour_mut().kademlia.set_mode(Some(Mode::Server));

    let mut stdin = io::BufReader::new(io::stdin()).lines();

    let mut ticker = time::interval(time::Duration::from_secs(5));

    swarm.listen_on("/ip4/0.0.0.0/tcp/0".parse()?)?;

    loop {
        select! {

        _ = ticker.tick() => {

            let key_count = swarm.behaviour_mut().kademlia.store_mut().records().count();
            if key_count > 0 {
                println!("Mempool size: {}", key_count);
            }
        }

        Ok(Some(line)) = stdin.next_line() => {
            handle_input_line(&mut swarm.behaviour_mut().kademlia, line);
        }
        event = swarm.select_next_some() => match event {
            SwarmEvent::NewListenAddr { address, .. } => {
                println!("Listening in {address:?}");
            },
            SwarmEvent::Behaviour(BehaviourEvent::Mdns(mdns::Event::Discovered(list))) => {
                for (peer_id, multiaddr) in list {
                    println!("Discovered peer: {peer_id:?} at {multiaddr}");
                    swarm.behaviour_mut().kademlia.add_address(&peer_id, multiaddr);

                    fetch_all_records(&mut swarm.behaviour_mut().kademlia, peer_id);
                }
            }
            SwarmEvent::Behaviour(BehaviourEvent::Kademlia(kad::Event::OutboundQueryProgressed { result, ..})) => {
                match result {
                    kad::QueryResult::GetProviders(Ok(kad::GetProvidersOk::FoundProviders { key, providers, .. })) => {
                        for peer in providers {
                            println!(
                                "Peer {peer:?} provides key {:?}",
                                std::str::from_utf8(key.as_ref()).unwrap()
                            );
                        }
                    }
                    kad::QueryResult::GetProviders(Err(err)) => {
                        eprintln!("Failed to get providers: {err:?}");
                    }
                    kad::QueryResult::GetRecord(Ok(kad::GetRecordOk::FoundRecord(kad::PeerRecord {
                        record: kad::Record { key: _, value, .. },
                        ..
                    }))) => {
                        // println!("Got record with key: {:?}", str::from_utf8(key.as_ref()).unwrap());
                        match serde_json::from_slice::<RecordValue>(&value) {
                            Ok(_record_value) => {
                                //println!("Deserialized Record: {:?}", record_value);
                            }
                            Err(err) => {
                                eprintln!("Failed to deserialize record: {:?}", err);
                            }
                        }
                    }
                    kad::QueryResult::GetRecord(Err(err)) => {
                        eprintln!("Failed to get record: {err:?}");
                    }
                    kad::QueryResult::PutRecord(Ok(kad::PutRecordOk { key })) => {
                        println!(
                            "Successfully put record {:?}",
                            std::str::from_utf8(key.as_ref()).unwrap()
                        );
                    }
                    kad::QueryResult::PutRecord(Err(err)) => {
                        eprintln!("Failed to put record: {err:?}");
                    }
                    kad::QueryResult::StartProviding(Ok(kad::AddProviderOk { key })) => {
                        println!(
                            "Successfully put provider record {:?}",
                            std::str::from_utf8(key.as_ref()).unwrap()
                        );
                    }
                    kad::QueryResult::StartProviding(Err(err)) => {
                        eprintln!("Failed to put provider record: {err:?}");
                    }
                    _ => {}
                }
            }
            _ => {}
        }
        }
    }
}

fn fetch_all_records(kademlia: &mut kad::Behaviour<MemoryStore>, peer_id: libp2p::PeerId) {
    let keys: Vec<_> = kademlia
        .store_mut()
        .records()
        .map(|record| record.key.clone())
        .collect();

    for key in keys {
        println!("Requesting record with key: {:?} from: {:?}", key, peer_id);
        kademlia.get_record(key);
    }
}

fn handle_input_line(kademlia: &mut kad::Behaviour<MemoryStore>, line: String) {
    let mut args = line.split(' ');

    match args.next() {
        Some("GET") => {
            let key = {
                match args.next() {
                    Some(key) => kad::RecordKey::new(&key),
                    None => {
                        eprintln!("Expected key");
                        return;
                    }
                }
            };
            kademlia.get_record(key);
        }
        Some("GET_PROVIDERS") => {
            let key = {
                match args.next() {
                    Some(key) => kad::RecordKey::new(&key),
                    None => {
                        eprintln!("Expected key");
                        return;
                    }
                }
            };
            kademlia.get_providers(key);
        }
        Some("PUT") => {
            let key = {
                match args.next() {
                    Some(key) => kad::RecordKey::new(&key),
                    None => {
                        eprintln!("Expected key");
                        return;
                    }
                }
            };
            let value = RecordValue {
                timestamp: 1622505600, // Example timestamp
                metadata: "Example metadata".to_string(),
                owner: "OwnerName".to_string(),
                source: "SourceName".to_string(),
                category: "CategoryName".to_string(),
            };

            let serialized_value = serde_json::to_vec(&value).expect("Failed to serialize value");

            let record = kad::Record {
                key,
                value: serialized_value,
                publisher: None,
                expires: None,
            };
            kademlia
                .put_record(record, kad::Quorum::One)
                .expect("Failed to store record locally.");
        }
        Some("PUT_PROVIDER") => {
            let key = {
                match args.next() {
                    Some(key) => kad::RecordKey::new(&key),
                    None => {
                        eprintln!("Expected key");
                        return;
                    }
                }
            };

            kademlia
                .start_providing(key)
                .expect("Failed to start providing key");
        }
        _ => {
            eprintln!("expected GET, GET_PROVIDERS, PUT or PUT_PROVIDER");
        }
    }
}
