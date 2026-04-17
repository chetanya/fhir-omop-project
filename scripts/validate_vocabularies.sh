#!/bin/bash
# scripts/validate_vocabularies.sh
# Check that OMOP vocabularies have been extracted correctly

VOCAB_DIR="data/vocabularies"

if [ ! -d "$VOCAB_DIR" ]; then
    echo "❌ Vocabularies directory not found: $VOCAB_DIR"
    echo "Please download from https://athena.ohdsi.org and extract to $VOCAB_DIR"
    exit 1
fi

echo "=== OMOP Vocabulary Check ==="
echo ""

# Check for required CSV files
required_files=("CONCEPT.csv" "CONCEPT_RELATIONSHIP.csv")
missing=0

for file in "${required_files[@]}"; do
    if [ -f "$VOCAB_DIR/$file" ]; then
        rows=$(wc -l < "$VOCAB_DIR/$file")
        echo "✓ $file ($((rows - 1)) rows)"
    else
        echo "❌ Missing: $file"
        missing=$((missing + 1))
    fi
done

if [ $missing -gt 0 ]; then
    echo ""
    echo "⚠️ Some vocabulary files are missing. Download from https://athena.ohdsi.org"
    exit 1
fi

echo ""
echo "✓ Vocabulary files present and ready to load"
echo ""
echo "Next: Run notebook 09_vocabulary_setup.py in Databricks to load into Delta tables"
