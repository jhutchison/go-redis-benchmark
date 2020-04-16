### Very simple benchmarking tool for Redis

This tool is intended to be similar to `redis-benchmark` with a few additional capabilities:

* Able to specify multiple hosts in order for client connections to be round-robined

### Usage

    Usage of ./rbm:
      -c int
            number of clients to use (default 2)
      -h string
            comma-separated host:port list (default "localhost:6379")
      -help
            help
      -i int
            iterations of the test to run - divided among clients (default 100000)
      -t string
            benchmark to run (default "sadd")
      -x int
            variant 1 - test dependent (default 1)
      -y int
            variant 2 - test dependent (default 1)
            
### Commands supported

#### `sadd` benchmark test

`sadd` Uses both variants to adjust the test characteristics

- `-x int` Randomized value to select a given set to use for each operation
- `-y int` Randomized value to define the member to add to the set

### Building

You will need go 1.13 to build. On Mac OS the easiest way to get go is simply to `brew install golang`

Build the utility with:

    $ go build

To cross-compile for another platform (example build a Linux exe on Mac)

    $ GOOS=linux GOARCH=amd64 go build

