# default target cuz gradescope cant install by default
all: 
	apt update && apt install -y curl build-essential
	curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
	. $$HOME/.cargo/env && cargo build --release -j 1 && \
	cargo build -p send && \
	cp target/debug/send 4700send && \
	cargo build -p recv && \
	cp target/debug/recv 4700recv

.PHONY: bins clean 4700send 4700recv
bins: 4700send 4700recv

4700send:
	cargo build -p send
	cp target/debug/send 4700send

4700recv:
	cargo build -p recv
	cp target/debug/recv 4700recv

clean:
	rm 4700send 4700recv
