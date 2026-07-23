#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
RESULTS_ROOT="${SCRIPT_DIR}/results"
PAGES_DIR="${REPO_ROOT}/pages"
COMMIT_MESSAGE="${1:-Update benchmark pages}"

cd "${REPO_ROOT}"

if command -v python3 >/dev/null 2>&1; then
  PYTHON=python3
elif command -v python >/dev/null 2>&1; then
  PYTHON=python
else
  echo "Python is required to build the benchmark Pages site." >&2
  exit 2
fi

"${PYTHON}" "${SCRIPT_DIR}/build-pages-site.py" --results "${RESULTS_ROOT}" --out "${PAGES_DIR}"

git add pages
if git diff --cached --quiet -- pages; then
  echo "No benchmark Pages changes to publish."
  exit 0
fi

git commit -m "${COMMIT_MESSAGE}"
git push

echo "Pushed benchmark Pages update. GitHub Actions will deploy it."
