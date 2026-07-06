#!/usr/bin/env bash
set -euo pipefail

BRANCH=""
TEST_TARGET=""
ROWS=""
RUNS=""

usage() {
    echo "Usage: $0 [ -b BRANCH ] [ -t TEST ] [ -r ROWS ] [ -n RUNS ]"
    echo "  -b BRANCH   Branch to pull and build (default: prompt)"
    echo "  -t TEST     Test to run: all | perf | <ClassName> (default: prompt)"
    echo "  -r ROWS     Parquet rows to generate (perf only, default: 100000)"
    echo "  -n RUNS     Benchmark iterations (perf only, default: 3)"
    echo ""
    echo "Examples:"
    echo "  $0 -b main -t all"
    echo "  $0 -b duckdb-only-no-acero -t perf"
    echo "  $0 -b duckdb-only-no-acero -t perf -r 500000 -n 5"
    echo "  $0 -b duckdb-only-no-acero -t ArrowFlightPerfTest"
    exit 1
}

while getopts "b:t:r:n:h" opt; do
    case "$opt" in
        b) BRANCH="$OPTARG" ;;
        t) TEST_TARGET="$OPTARG" ;;
        r) ROWS="$OPTARG" ;;
        n) RUNS="$OPTARG" ;;
        h) usage ;;
        *) usage ;;
    esac
done

# ── prompt for branch ─────────────────────────────────────────────────
if [ -z "$BRANCH" ]; then
    echo "Available branches:"
    git branch -a | sed 's/^..//;s/.*\///' | sort -u | head -20
    echo ""
    read -r -p "Branch to pull and build: " BRANCH
fi

# ── force pull: reset to remote ───────────────────────────────────────
echo ""
echo "=== Fetching origin/$BRANCH ==="
git fetch origin "$BRANCH"

echo "=== Force-resetting to origin/$BRANCH ==="
git checkout --force "$BRANCH" 2>/dev/null || git checkout -b "$BRANCH" origin/"$BRANCH" 2>/dev/null
git reset --hard origin/"$BRANCH"

# ── compile ───────────────────────────────────────────────────────────
echo ""
echo "=== Compiling ==="
mvn compile

# ── test selection ────────────────────────────────────────────────────
if [ -z "$TEST_TARGET" ]; then
    echo ""
    echo "Test options:"
    echo "  all    - run all tests"
    echo "  perf   - run ArrowFlightPerfTest (perf tag)"
    echo "  <name> - run specific test class (e.g. ArrowFlightPerfTest)"
    echo ""
    read -r -p "Test target [perf]: " TEST_TARGET
    TEST_TARGET="${TEST_TARGET:-perf}"
fi

# ── build Maven args ──────────────────────────────────────────────────
MAVEN_OPTS=()
if [ -n "$ROWS" ]; then
    MAVEN_OPTS+=("-Dperf.rows=$ROWS")
fi
if [ -n "$RUNS" ]; then
    MAVEN_OPTS+=("-Dperf.runs=$RUNS")
fi

# ── run tests ─────────────────────────────────────────────────────────
echo ""
echo "=== Running tests ==="

case "$TEST_TARGET" in
    all)
        mvn test "${MAVEN_OPTS[@]}"
        ;;
    perf)
        mvn test "${MAVEN_OPTS[@]}" "-DexcludedGroups=" "-Dgroups=perf" "-Dtest=ArrowFlightPerfTest"
        ;;
    int|integration)
        mvn test "${MAVEN_OPTS[@]}" "-DexcludedGroups=" "-Dtest=$TEST_TARGET"
        ;;
    *)
        mvn test "${MAVEN_OPTS[@]}" "-DexcludedGroups=" "-Dtest=$TEST_TARGET"
        ;;
esac
