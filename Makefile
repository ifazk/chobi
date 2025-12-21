.PHONY: check build test test_help

check:
	cargo fmt --check
	cargo check
	cargo clippy

build: check
	cargo build --release
	# statically compiling for x86_64 is our differentiator from syncoid
	ls -lah target/x86_64-unknown-linux-musl/release/chithi

TEST_ARGS=

test: check
	cargo run --bin chithi -- ${TEST_ARGS}

test_help: check
	cargo run --bin chithi -- -h source target
