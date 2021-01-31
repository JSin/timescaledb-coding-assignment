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

// GetConnectionString returns the TimeScaleDB connection string
func GetConnectionString(username, password, host, port, dbName string, maxConnections int) string {
	return fmt.Sprintf("postgres://%v:%v@%v:%v/%v?pool_max_conns=%d", username, password, host, port, dbName, maxConnections)
}

func main() {
	wFlag := flag.Int("w", 1, "Number of workers to initialize")
	flag.Parse()

	if len(os.Args) < 2 {
		fmt.Println("Missing file input")
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

	queryFile, err := os.Open(os.Args[1])
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
	fmt.Println(result)
}