#!/usr/bin/env bash

gzip --stdout --decompress data/dump.sql.gz | docker exec -i gh-analysis-db psql -U github -d github
