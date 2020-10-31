# THIS REPO:

````shell script
docker-compose up -d
docker exec -i pg-github-analysis psql -U github -d github < schema.sql
````

# GO PROJECT:

- install go

````shell script
go get github.com/benfred/github-analysis
cd $GOPATH/src/github.com/benfred/github-analysis
go install
````
- fix int64 errors => TODO created fixed fork
- `go install`
- `./build.sh`


## download raw github archive data
to download parallel: 
1. adjust months in source
2. ./build.sh
3. `$GOPATH/bin/gha-download-files`


## compact, for each day, all PushEvent events into a reduced tsv (csv with tabs)
```shell script
$GOPATH/bin/gha-parse-githubarchive -path /Users/ueli/repos/gh-geo-activity/data/gh-archive
```


## optional: Drop and recreate using schema OR just truncate
```shell script
docker exec -i gh-analysis-db psql -U github -d github -c "DROP TABLE events;"
docker exec -i gh-analysis-db psql -U github -d github < data/schema.sql
```


### import all tsv data into event table
`./import-gh-archive-tsv.sh`
REINDEX TABLE events;


## get user details via github api, depends on tsv (THIS REPO)
REINDEX TABLE locations;
```
--script github-user-location-scraper
```
REINDEX TABLE users;

## geocode user locations, depends on user-scraper
````shell script
# go run cmd/gha-location-scraper/main.go
$GOPATH/bin/gha-location-scraper
````
=> should use https://wiki.openstreetmap.org/wiki/Nominatim#Alternatives_.2F_Third-party_providers



# THIS REPO:

## NOAA location scraper
```
--script noaa-location-scraper
```

NOAA cities and countries are stored as json files in data directory.
Refresh them by running this projects main class.

````shell script
docker exec -i gh-analysis-db psql -U github -d github < data/import-countries-json.sql
docker exec -i gh-analysis-db psql -U github -d github < data/import-cities-json.sql
docker exec -i gh-analysis-db psql -U github -d github < data/views.sql
````

## Event weather scraper, depends on user-location-scraper & noaa location scraper

```
--script event-weather-scraper
```
