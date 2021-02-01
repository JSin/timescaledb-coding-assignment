# TimescaleDB Coding Assignment

Take home assignment from TimescaleDB. [Assignment PDF](assigment/cloud-coding-assignment.pdf) can be in the assignment folder.

## Expected Requirements
- Golang >= 1.15.7
- Docker-Compose >= 1.27.0

## Setup
```bash
docker-compose up -d # Spin up TimescaleDB
go build # builds command in the current working diretory
```

## Usage
```bash
./timescaledb-coding-assingment [options] filename
```

### Options
```bash
-w
```
The number of workers to create

### Example
```bash
./timescaledb-coding-assignment -w 5 inputs/query_params.csv
```

## Troubleshooting
**Q. TimescaleDB port in-use when running `docker-compose up`**\
A. Feel free to change the local interface port in the `docker-compose.yml`. If you go this route don't forget to update the `main.go` with the new Database Port. Ex. change `- 127.0.0.1:5432:5432` to `- 127.0.0.1:8080:5432`

## Assignment Assumptions
- Workers can run on same machine and do not need to be distributed.
- Workers can run on green threads.
- This command line tool will only be working with UTF8 encoding (or compatible formats).
- Using latest TimescaleDB version instead of assignment recommended v0.9.
- CSV will always contain three fields in the same order.
- CSV will always have a header field.
- CSV line lengths will not surpass Golang's read.io limits.
- Routing algorithm for determining which worker to run which hosts is chosen through a circular list
- Arbitrarily chose the number of db pool max connections (10).
- Arbitrarily chose the query queue buffer size (6000).
- Measured how TimescaleDB query took to respond not how long it took Golang to process the result.
- Measuring query time programmatically isn't accurate with multiple workers as you can not guarantee execution order. I.e. I can't guarantee that the worker will respond as soon as TimescaleDB responds with an answer.
- Best to use stats provided by DB tooling.
- I made the assumption that the input CSV might be extremely large so I decided to limit processing line by line (max 6000 queries at a time); however, I also made the decision that the results (i.e. query times) should fit into memory.