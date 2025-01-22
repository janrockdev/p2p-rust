use std::error::Error;
use std::time::Instant;
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

        Ok(Some(_line)) = stdin.next_line() => {
            time::sleep(time::Duration::from_secs(1)).await;
            // Benchmark configuration
            let total_operations = 1000;
            let mut times_put: Vec<u64> = Vec::new();
            //let mut times_get = Vec::new();
            for i in 0..total_operations {
                let key = kad::RecordKey::new(&format!("key-{i}"));
                let value = RecordValue {
                    timestamp: i as u64,
                    metadata: format!("Metadata {i}"),
                    owner: "OwnerName".to_string(),
                    source: "SourceName".to_string(),
                    category: "CategoryName".to_string(),
                };
                let serialized_value = serde_json::to_vec(&value).expect("Failed to serialize value");
                let start = Instant::now();
                let record = kad::Record {
                    key: key.clone(),
                    value: serialized_value,
                    publisher: None,
                    expires: None,
                };
                swarm
                    .behaviour_mut()
                    .kademlia
                    .put_record(record, kad::Quorum::One)
                    .expect("Failed to store record locally.");
                times_put.push(start.elapsed().as_micros() as u64);

                // // Measure GET operation
                // let start = Instant::now();
                // swarm.behaviour_mut().kademlia.get_record(key.clone());
                // times_get.push(start.elapsed().as_micros());
            }

            let avg_put_time: f64 = times_put.iter().sum::<u64>() as f64 / times_put.len() as f64;
            //let avg_get_time: f64 = times_get.iter().map(|&x| x as u64).sum::<u64>() as f64 / times_get.len() as f64;

            println!("Total PUT operations: {}", total_operations);
            println!("Average PUT time: {:.2} µs", avg_put_time);

            // println!("Total GET operations: {}", total_operations);
            // println!("Average GET time: {:.2} µs", avg_get_time);
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
                        record: kad::Record { key, value, .. },
                        ..
                    }))) => {
                        println!("Got record with key: {:?}", str::from_utf8(key.as_ref()).unwrap());
                        match serde_json::from_slice::<RecordValue>(&value) {
                            Ok(record_value) => {
                                println!("Deserialized Record: {:?}", record_value);
                            }
                            Err(err) => {
                                eprintln!("Failed to deserialize record: {:?}", err);
                            }
                        }
                    }
                    kad::QueryResult::GetRecord(Err(err)) => {
                        eprintln!("Failed to get record: {err:?}");
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
