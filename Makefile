build:
	cargo build

run3: build
	./target/debug/p2p-rust 8080

run4: build
	./target/debug/p2p-rust 8081

run5: build
	./target/debug/p2p-rust 8082

run6: build
	./target/debug/p2p-rust 8083

run7: build
	./target/debug/p2p-rust 8084

client: 
	cargo run --bin client

.PHONY: proto