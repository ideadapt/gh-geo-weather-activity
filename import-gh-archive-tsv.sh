#!/usr/bin/env bash

set -e

for file in $(find ./data/gh-archive -name "parsed_events.tsv" | sort); do
  f=$(echo "$file" | cut -c8-)
  echo "$f"
  docker exec -i gh-analysis-db psql -U github -d github \
    -c "COPY events(type, createdat, repoid, reponame, userid, username, repolanguage, forkid, forkname) FROM '/tmp/input/${f}' DELIMITER E'\t' CSV"
  mv "$file" "${file/.tsv/.done.tsv}"
done;
