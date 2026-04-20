MAELSTROM_VER := 0.2.3
BIN_DIR := .bin
MAELSTROM := $(BIN_DIR)/maelstrom

NODE_COUNT ?= 1
CONCURRENCY ?= 5
TIME_LIMIT ?= 20
RATE ?= 100
NEMESIS_INTERVAL ?= 5
# For maelstrom v0.2.3 txn-rw-register, supported nemesis is typically only: partition.
NEMESIS_OMISSION ?= partition
# Crash-style nemesis is workload-dependent; override this if your workload supports it.
NEMESIS_CRASH ?= partition

$(MAELSTROM):
	mkdir -p $(BIN_DIR)
	curl -sSL -o $(BIN_DIR)/maelstrom.tar.bz2 https://github.com/jepsen-io/maelstrom/releases/download/v$(MAELSTROM_VER)/maelstrom.tar.bz2
	tar -xjf $(BIN_DIR)/maelstrom.tar.bz2 -C $(BIN_DIR) --strip-components=1
	rm $(BIN_DIR)/maelstrom.tar.bz2

build:
	go build -o node .

# Transaction workload: Read-write register with transaction semantics
test-rw-register: $(MAELSTROM) build
	$(MAELSTROM) test -w txn-rw-register \
	  --bin ./node \
	  --node-count $(NODE_COUNT) \
	  --concurrency $(CONCURRENCY) \
	  --time-limit $(TIME_LIMIT) \
	  --rate $(RATE)

# Raft replication workload: read-write register with Maelstrom-backed replication
test-raft-register: $(MAELSTROM) build
	$(MAELSTROM) test -w txn-rw-register \
	  --bin ./node \
	  --node-count $(NODE_COUNT) \
	  --concurrency $(CONCURRENCY) \
	  --time-limit $(TIME_LIMIT) \
	  --rate $(RATE) \
	  -- \
	  --replication-strategy raft \
	  --replication-backend maelstrom

# Raft workload with omission fault injection (message drops/omissions).
test-raft-omission: $(MAELSTROM) build
	$(MAELSTROM) test -w txn-rw-register \
	  --bin ./node \
	  --node-count $(NODE_COUNT) \
	  --concurrency $(CONCURRENCY) \
	  --time-limit $(TIME_LIMIT) \
	  --rate $(RATE) \
	  --nemesis $(NEMESIS_OMISSION) \
	  --nemesis-interval $(NEMESIS_INTERVAL) \
	  --log-stderr \
	  --log-net-send \
	  --log-net-recv \
	  -- \
	  --replication-strategy raft \
	  --replication-backend maelstrom

# Raft workload with node-fault simulation. For txn-rw-register this defaults to partition.
test-raft-crash: $(MAELSTROM) build
	@echo "Using nemesis: $(NEMESIS_CRASH) (set NEMESIS_CRASH=... to override)"
	$(MAELSTROM) test -w txn-rw-register \
	  --bin ./node \
	  --node-count $(NODE_COUNT) \
	  --concurrency $(CONCURRENCY) \
	  --time-limit $(TIME_LIMIT) \
	  --rate $(RATE) \
	  --nemesis $(NEMESIS_CRASH) \
	  --nemesis-interval $(NEMESIS_INTERVAL) \
	  --log-stderr \
	  --log-net-send \
	  --log-net-recv \
	  -- \
	  --replication-strategy raft \
	  --replication-backend maelstrom

# Generic Raft fault harness: pass NEMESIS=... at invocation time.
test-raft-fault: $(MAELSTROM) build
	@if [ -z "$(NEMESIS)" ]; then \
	  echo "NEMESIS is required. Example: make test-raft-fault NEMESIS=partition"; \
	  exit 1; \
	fi
	$(MAELSTROM) test -w txn-rw-register \
	  --bin ./node \
	  --node-count $(NODE_COUNT) \
	  --concurrency $(CONCURRENCY) \
	  --time-limit $(TIME_LIMIT) \
	  --rate $(RATE) \
	  --nemesis $(NEMESIS) \
	  --nemesis-interval $(NEMESIS_INTERVAL) \
	  --log-stderr \
	  --log-net-send \
	  --log-net-recv \
	  -- \
	  --replication-strategy raft \
	  --replication-backend maelstrom

# Multi-node default: validated Raft path for node-count > 1
test-multinode: test-raft-register

# Strong consistency workload: synchronous replication with Maelstrom-backed transport
test-strong-consistency-register: $(MAELSTROM) build
	$(MAELSTROM) test -w txn-rw-register \
	  --bin ./node \
	  --node-count $(NODE_COUNT) \
	  --concurrency $(CONCURRENCY) \
	  --time-limit $(TIME_LIMIT) \
	  --rate $(RATE) \
	  -- \
	  --replication-strategy strong-consistency \
	  --replication-backend maelstrom

# Stress test: High concurrency with txn-list-append
test-stress: $(MAELSTROM) build
	$(MAELSTROM) test -w txn-list-append \
	  --bin ./node \
	  --node-count $(NODE_COUNT) \
	  --concurrency $(CONCURRENCY) \
	  --time-limit $(TIME_LIMIT) \
	  --rate $(RATE)

# Default test: Run transaction tests using the Raft-backed path
test: test-raft-register

# Print available test CLI options and nemesis types for your local Maelstrom binary.
help-options: $(MAELSTROM)
	$(MAELSTROM) test --help

clean:
	rm -rf $(BIN_DIR) node
