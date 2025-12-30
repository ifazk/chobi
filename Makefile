.PHONY: check build build_linux build_freebsd test test_help

check:
	cargo fmt --check
	cargo check
	cargo clippy

build: build_linux build_freebsd

build_linux: check
	cargo build --release --target x86_64-unknown-linux-musl
	file target/x86_64-unknown-linux-musl/release/chithi
	ls -lah target/x86_64-unknown-linux-musl/release/chithi

build_freebsd: check
	cargo build --release --target x86_64-unknown-freebsd
	file target/x86_64-unknown-freebsd/release/chithi
	ls -lah target/x86_64-unknown-freebsd/release/chithi

TEST_ARGS=

test: check
	cargo run --bin chithi -- ${TEST_ARGS}

test_help: check
	cargo run --bin chithi -- -h source target
