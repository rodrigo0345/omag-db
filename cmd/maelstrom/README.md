# Maelstrom Runner

This folder contains a local Maelstrom test harness for the OMAG node binary in `cmd/maelstrom`.

## CLI Options

A full list of options is available by running:

```bash
cd /home/rodrigo0345/omag/cmd/maelstrom
make help-options
```

Important options include:

- `--workload NAME`: workload to run.
- `--bin SOME_BINARY`: binary Maelstrom should spawn.
- `--node-count N`: number of nodes to spawn.

Useful debugging flags:

- `--log-stderr`: show STDERR output from each node.
- `--log-net-send`: log messages entering the network.
- `--log-net-recv`: log messages received by nodes.

Aggressiveness and fault controls:

- `--time-limit SECONDS`
- `--rate FLOAT`
- `--concurrency INT`
- `--latency MILLIS`
- `--latency-dist DIST`
- `--nemesis FAULT_TYPE`
- `--nemesis-interval SECONDS`

Workload-specific knobs:

- Broadcast workloads: `--topology TYPE`
- Transactional workloads:
  - `--max-txn-length INT`
  - `--key-count INT`
  - `--max-writes-per-key INT`

Maelstrom runs locally; SSH options are not used in this setup.

## Fault-Injection Targets

The Makefile includes Raft-focused fault targets:

- `make test-raft-omission` (default nemesis: `partition`)
- `make test-raft-crash` (defaults to `partition`; override `NEMESIS_CRASH=...`)
- `make test-raft-fault NEMESIS=<type>` (custom fault)

Note: with Maelstrom `v0.2.3` and workload `txn-rw-register`, the accepted nemesis in this environment is `partition`.

Example:

```bash
cd /home/rodrigo0345/omag/cmd/maelstrom
make test-raft-omission NODE_COUNT=5 CONCURRENCY=20 TIME_LIMIT=60 RATE=200
make test-raft-crash NODE_COUNT=5 NEMESIS_INTERVAL=3
make test-raft-fault NEMESIS=partition NODE_COUNT=5
```

