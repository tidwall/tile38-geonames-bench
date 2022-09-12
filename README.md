# tile38-geonames-bench

A simple benchmark tool for Tile38 using Geoname points.

Make sure that Tile38 is running somewhere. 
By default this program connects to `localhost:9851`.

Also it may be a good idea to run Tile38 without the AOF running such 
as `tile38-server --appendonly no`.

## Installing

```
$ go install github.com/tidwall/tile38-geonames-bench
```

## Running

```
$ tile38-geonames-bench
```

