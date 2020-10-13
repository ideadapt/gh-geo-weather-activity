#!/usr/bin/env bash

for file in $(find . -name "*.tsv"); do
  f=$(echo "$file" | cut -c8-)
  echo "$f"
  docker exec -i gh-analysis-db psql -U github -d github \
    -c "COPY events(type, createdat, repoid, reponame, userid, username, repolanguage, forkid, forkname) FROM '/tmp/input/${f}' DELIMITER E'\t' CSV"
done;
