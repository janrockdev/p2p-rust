build:
	cargo build

run: build
	./target/debug/p2p-rust

client: 
	cargo run --bin client

.PHONY: proto