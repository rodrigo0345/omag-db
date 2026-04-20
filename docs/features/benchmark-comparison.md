# Engine vs PostgreSQL Benchmark

This report compares OMAG's SQL engine against a stock PostgreSQL container using the same simple-query workload profile.

## What it measures

- Point reads by primary key
- `WHERE`-filtered reads using equality predicates joined by `AND`
- Insert-heavy write workloads
- Deletes against seeded rows
- A mixed workload that interleaves reads, filters, inserts, and deletes

## How to run it

From the repository root:

```bash
go run ./cmd/benchmark
```

The command will:

1. Build the OMAG engine container from `Dockerfile`
2. Start PostgreSQL with default container settings
3. Seed both databases with the same table and rows
4. Run the benchmark workloads over both backends
5. Write the final PDF report to `docs/papers/engine-vs-postgres-benchmark.pdf`

You can adjust the workload size with flags such as:

```bash
go run ./cmd/benchmark -seed-rows 512 -warmup-ops 64 -ops 256
```

## Report

The latest generated report is stored at:

- [`docs/papers/engine-vs-postgres-benchmark.pdf`](../papers/engine-vs-postgres-benchmark.pdf)



