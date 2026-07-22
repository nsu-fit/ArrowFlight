#!/usr/bin/env bash
set -euo pipefail

BRANCH=""
TEST_TARGET=""
ROWS=""
RUNS=""
FORCE_RESET=0

usage() {
    echo "Usage: $0 [ -b BRANCH ] [ -t TEST ] [ -r ROWS ] [ -n RUNS ] [ --force-reset ]"
    echo "  -b BRANCH       Branch to pull and build (default: prompt)"
    echo "  -t TEST         Test to run: all | perf | <ClassName> (default: prompt)"
    echo "  -r ROWS         Parquet rows to generate (perf only, default: 100000)"
    echo "  -n RUNS         Benchmark iterations (perf only, default: 3)"
    echo "  --force-reset   Destructively reset to origin/BRANCH after confirmation if dirty"
    echo ""
    echo "Examples:"
    echo "  $0 -b main -t all"
    echo "  $0 -b feature/goto-duckdb -t perf"
    echo "  $0 -b feature/goto-duckdb -t perf -r 500000 -n 5"
    echo "  $0 -b feature/goto-duckdb -t ArrowFlightPerfTest"
    echo "  $0 -b main -t all --force-reset"
    exit 1
}

ARGS=()
while [ "$#" -gt 0 ]; do
    case "$1" in
        --force-reset)
            FORCE_RESET=1
            ;;
        --help)
            usage
            ;;
        *)
            ARGS+=("$1")
            ;;
    esac
    shift
done
set -- "${ARGS[@]}"

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

is_worktree_dirty() {
    [ -n "$(git status --porcelain)" ]
}

confirm_force_reset_if_dirty() {
    if ! is_worktree_dirty; then
        return
    fi

    echo ""
    echo "Working tree has local changes:"
    git status --short
    echo ""
    echo "A force reset will discard tracked local changes while switching to origin/$BRANCH."
    read -r -p "Type 'reset $BRANCH' to continue: " CONFIRM
    if [ "$CONFIRM" != "reset $BRANCH" ]; then
        echo "Aborted."
        exit 1
    fi
}

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

if [ "$FORCE_RESET" -eq 1 ]; then
    confirm_force_reset_if_dirty
    echo "=== Force-resetting to origin/$BRANCH ==="
    git checkout --force "$BRANCH" 2>/dev/null || git checkout -b "$BRANCH" origin/"$BRANCH" 2>/dev/null
    git reset --hard origin/"$BRANCH"
else
    echo "=== Checking out $BRANCH without discarding local changes ==="
    git checkout "$BRANCH" 2>/dev/null || git checkout -b "$BRANCH" origin/"$BRANCH"
    echo "=== Fast-forwarding to origin/$BRANCH ==="
    git merge --ff-only origin/"$BRANCH"
fi

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
