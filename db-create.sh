#!/usr/bin/env bash

docker exec -i gh-analysis-db psql -U github < db.sql
docker exec -i gh-analysis-db psql -U github -d github < schema.sql
