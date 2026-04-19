MAELSTROM_VER := 0.2.3
BIN_DIR := .bin
MAELSTROM := $(BIN_DIR)/maelstrom

NODE_COUNT ?= 1
CONCURRENCY ?= 5
TIME_LIMIT ?= 20
RATE ?= 100

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

# Default test: Run transaction tests
test: build test-rw-register

clean:
	rm -rf $(BIN_DIR) node
