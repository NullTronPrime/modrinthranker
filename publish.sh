#!/usr/bin/env bash
set -uo pipefail

git config user.name "modrinthranker bot"
git config user.email "actions@users.noreply.github.com"
git add docs

if git diff --cached --quiet; then
  echo "publish: nothing changed"
  exit 0
fi

git commit -m "${1:-census}"
remote="https://x-access-token:${GITHUB_TOKEN}@github.com/${GITHUB_REPOSITORY}"

git fetch --quiet origin main
if git rebase origin/main >/dev/null 2>&1; then
  if git push "$remote" HEAD:main; then
    echo "publish: pushed cleanly"
    exit 0
  fi
fi

git rebase --abort >/dev/null 2>&1 || true
echo "publish: main moved during the run, skipping so the next run republishes from the database"
exit 0