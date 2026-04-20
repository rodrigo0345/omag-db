#!/bin/bash
# Script to generate class diagrams from Go code
# Run this from the project root: ./scripts/generate-diagrams.sh

set -e

echo "=== GoPlantUML Class Diagram Generator ==="
echo ""

# Check if goplantuml is installed
if ! command -v goplantuml &> /dev/null; then
    echo "Installing GoPlantUML..."
    go install github.com/jfeliu007/goplantuml/cmd/goplantuml@latest
fi

# Create output directory
mkdir -p docs/diagrams

echo "Generating class diagrams..."
echo ""

# Transaction/Isolation system
echo "✓ Generating transaction package diagram..."
goplantuml -output docs/diagrams/txn_classes.puml ./internal/txn

echo "✓ Generating isolation strategies diagram..."
goplantuml -output docs/diagrams/isolation_classes.puml ./internal/isolation

# Storage system
echo "✓ Generating storage package diagram..."
goplantuml -output docs/diagrams/storage_classes.puml ./internal/storage

echo "✓ Generating B+ Tree implementation diagram..."
goplantuml -output docs/diagrams/btree_classes.puml ./internal/storage/btree

echo "✓ Generating LSM Tree implementation diagram..."
goplantuml -output docs/diagrams/lsm_classes.puml ./internal/storage/lsm

echo "✓ Generating buffer pool diagram..."
goplantuml -output docs/diagrams/buffer_classes.puml ./internal/storage/buffer

# Concurrency
echo "✓ Generating concurrency control diagram..."
goplantuml -output docs/diagrams/concurrency_classes.puml ./internal/concurrency

# Full architecture
echo "✓ Generating full architecture diagram..."
goplantuml -output docs/diagrams/full_architecture.puml ./internal

echo ""
echo "=== Success! ==="
echo "Generated diagrams:"
ls -1 docs/diagrams/*.puml | sed 's/^/  • /'
echo ""
echo "View diagrams:"
echo "  • VS Code: Install 'PlantUML' extension"
echo "  • Online: http://www.plantuml.com/plantuml/uml/"
echo "  • Docs: Check docs/architecture/diagrams.md"
