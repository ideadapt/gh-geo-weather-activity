#!/usr/bin/env bash

docker exec gh-analysis-db pg_dump -U github -d github | gzip --best > data/dump.sql.gz
