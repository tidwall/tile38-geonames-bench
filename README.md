# tile38-geonames-bench

A simple benchmark tool for Tile38 using Geoname points.

Make sure that Tile38 is running somewhere. 
By default this program connects to `localhost:9851`.

Also it may be a good idea to run Tile38 without the AOF running such 
as `tile38-server --appendonly no`.

## Installing

```
$ go get github.com/tidwall/tile38-geonames-bench
```

## Running

```
$ tile38-geonames-bench
```

The first run may take a while because it needs to download the geonames `allCountries.zip` file and generate the `geonames.bin.gz` file.
