package main

import (
	"archive/zip"
	"bytes"
	"compress/gzip"
	"flag"
	"fmt"
	"io"
	"log"
	"math/rand"
	"os"
	"os/exec"
	"runtime"
	"strings"
	"time"

	_ "embed"

	"github.com/gomodule/redigo/redis"
	"github.com/tidwall/gjson"
	"github.com/tidwall/lotsa"
)

func main() {
	var addr string
	var noset bool
	flag.StringVar(&addr, "addr", ":9851", "Tile38 address")
	flag.BoolVar(&noset, "noset", false, "Do not load and set geonnames")
	flag.Parse()
	log.SetFlags(0)
	rand.Seed(time.Now().UnixNano())
	fmt.Printf("Connecting to Tile38 at %s...\n", addr)
	conn, err := redis.Dial("tcp", addr)
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	if !noset {
		fmt.Printf("Reading points from geonames.bin.gz into memory...\n")
		data, err := os.ReadFile("geonames.bin.gz")
		if err != nil {
			if os.IsNotExist(err) {
				if err := buildBin(); err != nil {
					log.Fatal(err)
				}
				data, err = os.ReadFile("geonames.bin.gz")
				if err != nil {
					log.Fatal(err)
				}
			} else {
				log.Fatal(err)
			}
		}
		r, err := gzip.NewReader(bytes.NewBuffer(data))
		if err != nil {
			log.Fatal(err)
		}
		data, err = io.ReadAll(r)
		if err != nil {
			log.Fatal(err)
		}
		lines := strings.Split(string(data), "\n")
		lines = lines[:len(lines)-1] // last line is empty
		fmt.Printf("Shuffling points...\n")
		rand.Shuffle(len(lines), func(i, j int) {
			lines[i], lines[j] = lines[j], lines[i]
		})
		const chunk = 2321
		var batch int
		var total int
		fmt.Printf("Setting points into Tile38...\n")
		start := time.Now()
		for i, line := range lines {
			cols := strings.Split(line, ",")
			err := conn.Send("SET", "geonames", cols[0], "POINT", cols[1], cols[2])
			if err != nil {
				log.Fatal(err)
			}
			batch++
			if i == len(lines)-1 || batch == chunk {
				if batch > 0 {
					err := conn.Flush()
					if err != nil {
						log.Fatal(err)
					}
					for i := 0; i < batch; i++ {
						reply, err := redis.String(conn.Receive())
						if err != nil {
							log.Fatal(err)
						}
						if reply != "OK" {
							log.Fatalf("expected 'OK', got '%s'", reply)
						}
					}
					total += batch
					fmt.Printf("\r%s / %s ",
						commaize(total), commaize(len(lines)))
					batch = 0
				}
			}
		}
		lotsa.WriteOutput(os.Stdout, total, 1, time.Since(start), 0)
	}
	conn.Do("OUTPUT", "json")
	lotsa.Output = os.Stdout
	T := runtime.NumCPU()
	var conns []redis.Conn
	for i := 0; i < T; i++ {
		conn, err := redis.Dial("tcp", addr)
		if err != nil {
			log.Fatal(err)
		}
		conn.Do("OUTPUT", "json")
		conns = append(conns, conn)
	}
	N := 10_000
	fmt.Printf("Sending %s random 10km WITHIN operations to Tile38...\n",
		commaize(N))
	lotsa.Ops(N, T, func(i, t int) {
		lat := rand.Float64()*180 - 90
		lon := rand.Float64()*360 - 180
		reply, err := redis.String(
			conns[t].Do("WITHIN", "geonames", "COUNT",
				"CIRCLE", lat, lon, 10000))
		if err != nil {
			log.Fatal(err)
		}
		if !gjson.Get(reply, "ok").Bool() {
			log.Fatal("WITHIN failed")
		}
	})

	conn.Do("GC")
	reply, err := redis.String(conn.Do("SERVER"))
	if err != nil {
		log.Fatal(err)
	}
	heap := int(gjson.Get(reply, "stats.heap_size").Int())
	avgItemSize := int(gjson.Get(reply, "stats.avg_item_size").Int())
	fmt.Printf("Heap Size: %s MB\n", commaize(heap/1024/1024))
	fmt.Printf("Avg Item: %s bytes\n", commaize(avgItemSize))
}

func commaize(n int) string {
	s1, s2 := fmt.Sprintf("%d", n), ""
	for i, j := len(s1)-1, 0; i >= 0; i, j = i-1, j+1 {
		if j%3 == 0 && j != 0 {
			s2 = "," + s2
		}
		s2 = string(s1[i]) + s2
	}
	return s2
}

// convert an "allCountries.txt" style file to a "geonames.bin.gz".
func buildBin() error {
	var odata []byte
	fmt.Printf("Reading allCountries.txt into memory...\n")
	data, err := os.ReadFile("allCountries.txt")
	if err != nil {
		if os.IsNotExist(err) {
			if err := downloadAllCountries(); err != nil {
				return err
			}
			data, err = os.ReadFile("allCountries.txt")
			if err != nil {
				return err
			}
		} else {
			return err
		}
	}
	lines := strings.Split(string(data), "\n")
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" {
			break
		}
		cols := strings.Split(line, "\t")
		odata = append(odata, cols[0]+","+cols[4]+","+cols[5]+"\n"...)
	}
	fmt.Printf("Creating geonames.bin.gz...\n")
	f, err := os.Create("geonames.bin.gz-tmp")
	if err != nil {
		return err
	}
	defer os.RemoveAll("geonames.bin.gz-tmp")
	defer f.Close()
	w, err := gzip.NewWriterLevel(f, gzip.BestCompression)
	if err != nil {
		return err
	}
	if _, err := w.Write(odata); err != nil {
		return err
	}
	if err := w.Flush(); err != nil {
		return err
	}
	if err := w.Close(); err != nil {
		return err
	}
	if err := os.Rename("geonames.bin.gz-tmp", "geonames.bin.gz"); err != nil {
		return err
	}
	os.RemoveAll("allCountries.zip")
	os.RemoveAll("allCountries.txt")
	return nil
}

func downloadAllCountries() error {
	fmt.Printf("Downloading allCountries.zip. Please wait...\n")
	url := "http://download.geonames.org/export/dump/allCountries.zip"
	os.RemoveAll("allCountries.zip-tmp")
	defer os.RemoveAll("allCountries.zip-tmp")
	cmd := exec.Command("wget", "-O", "allCountries.zip-tmp", url)
	cmd.Stderr = os.Stderr
	cmd.Stdout = os.Stdout
	if err := cmd.Run(); err != nil {
		return err
	}
	err := os.Rename("allCountries.zip-tmp", "allCountries.zip")
	if err != nil {
		return err
	}
	r, err := zip.OpenReader("allCountries.zip")
	if err != nil {
		return err
	}
	defer r.Close()
	for _, f := range r.File {
		if f.Name == "allCountries.txt" {
			fmt.Printf("Decompressing allCountries.txt...\n")
			err := func() error {
				r, err := f.Open()
				if err != nil {
					return err
				}
				defer r.Close()
				f, err := os.Create("allCountries.txt")
				if err != nil {
					return err
				}
				defer f.Close()
				if _, err := io.Copy(f, r); err != nil {
					return err
				}
				return nil
			}()
			if err != nil {
				return err
			}
		}
	}
	return nil
}
