package queries

import (
	"context"
	"fmt"
	"os"
	"sort"
	"sync"
	"time"

	"github.com/jackc/pgx/v4/pgxpool"
)

// QueryParams is the input needed to create query
type QueryParams struct {
	Host  string
	Start time.Time
	End   time.Time
}

type task struct {
	queryParams QueryParams
}

type worker struct {
	taskChan chan task
}

type queryResult struct {
	Bucket time.Time
	Max    float64
	Min    float64
}

// QueryTimesResult is the final result after processing the queries
type QueryTimesResult struct {
	NumOfQueries int
	TotalTime    time.Duration
	Min          time.Duration
	Median       time.Duration
	Avg          time.Duration
	Max          time.Duration
}

const query = `
	SELECT time_bucket('1 minute', ts) as one_min, max(usage), min(usage)
	FROM cpu_usage
	WHERE host = $1 AND ts >= $2 AND ts <= $3
	GROUP BY one_min
	ORDER BY one_min
`

// QueryDispatcher creates workers and dispatchs the queries to the correct worker
func QueryDispatcher(numOfWorkers int, queryCh chan QueryParams, wg *sync.WaitGroup, resultCh chan QueryTimesResult, dbpool *pgxpool.Pool) {
	hostWorkerMap := make(map[string]int)
	newWorkerPointer := 0
	workers := make([]worker, numOfWorkers, numOfWorkers)
	queryTimeNsChan := make(chan int64) // upper bounds based on queryCh
	queryTimesList := []int64{}

	go addQueryTimes(queryTimeNsChan, &queryTimesList, wg)

	for i := 0; i < numOfWorkers; i++ {
		workers[i] = worker{
			taskChan: make(chan task), // upper bounds based on queryCh
		}
		go workers[i].runQuery(queryTimeNsChan, dbpool, wg)
	}

	for queryParams := range queryCh {
		workerPointer, ok := hostWorkerMap[queryParams.Host]
		if !ok {
			hostWorkerMap[queryParams.Host] = newWorkerPointer
			workerPointer = newWorkerPointer
			newWorkerPointer = (newWorkerPointer + 1) % numOfWorkers
		}
		workers[workerPointer].taskChan <- task{
			queryParams,
		}
	}

	close(queryTimeNsChan)
	for _, w := range workers {
		close(w.taskChan)
	}

	result := processQueryTimes(queryTimesList)
	resultCh <- result
	close(resultCh)
}

func (w worker) runQuery(queryTimeNsChan chan int64, dbpool *pgxpool.Pool, wg *sync.WaitGroup) {
	for t := range w.taskChan {
		start := time.Now()
		rows, err := dbpool.Query(context.Background(), query, t.queryParams.Host, t.queryParams.Start, t.queryParams.End)
		queryTime := time.Since(start)
		if err != nil {
			fmt.Fprintln(os.Stderr, "Could not execute query:", err)
			rows.Close()
			wg.Done()
			continue
		}
		queryTimeNsChan <- queryTime.Nanoseconds()
		// No need for the results
		// for rows.Next() {
		// 	var r queryResult
		// 	err = rows.Scan(&r.Bucket, &r.Max, &r.Min)
		// 	if err != nil {
		// 		fmt.Fprintln(os.Stderr, "Unable to scan rows:", err)
		// 		break
		// 	}
		// }
		// if rows.Err() != nil {
		// 	fmt.Fprintln(os.Stderr, "Row error:", err)
		// }
		rows.Close()
	}
}

func addQueryTimes(queryTimeNsChan chan int64, queryTimesList *[]int64, wg *sync.WaitGroup) {
	for qT := range queryTimeNsChan {
		*queryTimesList = append(*queryTimesList, qT)
		wg.Done()
	}
}

func processQueryTimes(queryTimesList []int64) QueryTimesResult {
	if len(queryTimesList) == 0 {
		return QueryTimesResult{}
	}
	sort.Slice(queryTimesList, func(i, j int) bool { return queryTimesList[i] < queryTimesList[j] })
	queries := len(queryTimesList)
	var totalTime int64
	for _, val := range queryTimesList {
		totalTime += val
	}
	var median int64
	mid := queries / 2
	if (queries & 1) == 0 {
		median = (queryTimesList[mid-1] + queryTimesList[mid]) / 2 // lost precision
	} else {
		median = queryTimesList[mid]
	}
	return QueryTimesResult{
		NumOfQueries: queries,
		TotalTime:    time.Duration(totalTime),
		Min:          time.Duration(queryTimesList[0]),
		Median:       time.Duration(median),
		Avg:          time.Duration(totalTime / int64(queries)), // lost precision
		Max:          time.Duration(queryTimesList[len(queryTimesList)-1]),
	}
}
