test:
	RUST_LOG=debug cargo run --bin chithi

test_help:
	RUST_LOG=debug cargo run --bin chithi -- -h source target