.PHONY: test test_help

test:
	RUST_LOG=debug cargo run --bin chithi ${TEST_ARGS}

test_help:
	RUST_LOG=debug cargo run --bin chithi -- -h source target