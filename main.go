package main

import (
	"context"
	"encoding/csv"
	"flag"
	"fmt"
	"io"
	"os"
	"sync"
	"time"

	"github.com/JSin/timescaledb-coding-assignment/pkg/queries"
	"github.com/jackc/pgx/v4/pgxpool"
)

const (
	dbUsername                = "postgres"
	dbPassword                = "password"
	dbHost                    = "127.0.0.1"
	dbport                    = "5432"
	dbName                    = "homework"
	dbMaxConnections          = 10
	queryProcessingBufferSize = 6000
	layoutTime                = "2006-01-02 15:04:05"
)

const resultString = `
Number of Queries Processed: %d
Total Processing Time:       %v
The Minimum Query Time:      %v
The Median Query Time:       %v
The Average Query Time:      %v
The Maximum Query TIme:      %v
`

// GetConnectionString returns the TimeScaleDB connection string
func GetConnectionString(username, password, host, port, dbName string, maxConnections int) string {
	return fmt.Sprintf("postgres://%v:%v@%v:%v/%v?pool_max_conns=%d", username, password, host, port, dbName, maxConnections)
}

func main() {
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage of %s [options] filename\n", os.Args[0])
		flag.PrintDefaults()
	}
	wFlag := flag.Int("w", 1, "Number of workers to initialize")
	flag.Parse()

	filename := flag.Arg(0)
	if filename == "" {
		fmt.Fprintln(os.Stderr, "Missing file input")
		os.Exit(1)
	} else if info, err := os.Stat(filename); os.IsNotExist(err) || info.IsDir() {
		fmt.Fprintln(os.Stderr, "Invalid file")
		os.Exit(1)
	}

	ctx := context.Background()
	connStr := GetConnectionString(dbUsername, dbPassword, dbHost, dbport, dbName, dbMaxConnections)
	dbpool, err := pgxpool.Connect(ctx, connStr)
	if err != nil {
		fmt.Fprintln(os.Stderr, "Unable to connect to database:", err)
		os.Exit(1)
	}
	defer dbpool.Close()

	var wg sync.WaitGroup
	queryCh := make(chan queries.QueryParams, queryProcessingBufferSize)
	resultCh := make(chan queries.QueryTimesResult)
	go queries.QueryDispatcher(*wFlag, queryCh, &wg, resultCh, dbpool)

	queryFile, err := os.Open(filename)
	if err != nil {
		fmt.Fprintln(os.Stderr, "Provided file cannot be open:", err)
		os.Exit(1)
	}
	isHeader := true
	csvReader := csv.NewReader(queryFile)
	for {
		line, err := csvReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			fmt.Fprintln(os.Stderr, "Error while reading input:", err)
			break
		}
		if isHeader {
			isHeader = false
			continue
		}
		if len(line) < 3 {
			fmt.Fprintln(os.Stderr, "CSV format is invalid")
			continue
		}
		start, err := time.Parse(layoutTime, line[1])
		if err != nil {
			fmt.Fprintln(os.Stderr, "Could not parse start time:", err)
			continue
		}
		end, err := time.Parse(layoutTime, line[2])
		if err != nil {
			fmt.Fprintln(os.Stderr, "Could not parse end time:", err)
			continue
		}
		wg.Add(1)
		queryCh <- queries.QueryParams{
			Host:  line[0],
			Start: start,
			End:   end,
		}
	}
	queryFile.Close()

	wg.Wait()
	close(queryCh)
	result := <-resultCh
	fmt.Printf(resultString, result.NumOfQueries, result.TotalTime, result.Min, result.Median, result.Avg, result.Max)
}
