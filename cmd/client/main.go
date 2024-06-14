package main

import (
	"flag"
	"fmt"
	"log/slog"
	"net"
	"os"
	"sync"
	"time"

	"github.com/gaukas/benchmarkconn"
)

const (
	defaultNetwork = "tcp"
)

var (
	network string
	raddr   string

	messageSize  int
	totalMessage int
	interval     time.Duration

	timeout time.Duration
)

func init() {
	flag.StringVar(&network, "network", defaultNetwork, "network type (tcp, unix, etc)")
	flag.StringVar(&network, "net", defaultNetwork, "network type (tcp, unix, etc) (shorthand)")
	flag.StringVar(&raddr, "raddr", "", "remote address to connect to")
	flag.StringVar(&raddr, "a", "", "local address to connect to (shorthand)")

	flag.IntVar(&messageSize, "message-size", 1024, "size of the message to send/expect")
	flag.IntVar(&messageSize, "sz", 1024, "size of the message to send/expect (shorthand)")
	flag.IntVar(&totalMessage, "total-message", 1000, "total number of messages to send/expect")
	flag.IntVar(&totalMessage, "m", 1000, "total number of messages to send/expect (shorthand)")
	flag.DurationVar(&interval, "interval", 1*time.Millisecond, "minimal interval between each message, ignored for type other than echo")
	flag.DurationVar(&interval, "i", 1*time.Millisecond, "minimal interval between each message, ignored for type other than echo (shorthand)")

	flag.DurationVar(&timeout, "timeout", 10*time.Second, "timeout for the client to wait for the benchmark to finish")
	flag.DurationVar(&timeout, "t", 10*time.Second, "timeout for the client to wait for the benchmark to finish (shorthand)")
}

func exitWithUsage() {
	flag.Usage()
	fmt.Println("To start the benchmark client: client type command [arguments...]")
	fmt.Printf("Possible type: pressure, echo\n")
	fmt.Printf("Possible commands: write, read\n")
	os.Exit(1)
}

func main() {
	flag.Parse()
	if flag.NArg() != 2 {
		exitWithUsage()
	}

	if raddr == "" {
		fmt.Println("remote address is required")
		exitWithUsage()
	}

	benchType := flag.Arg(0)
	command := flag.Arg(1)
	var commandWrite bool
	switch command {
	case "write":
		commandWrite = true
	case "read":
		commandWrite = false
	default:
		fmt.Printf("unknown command: %s\n", command)
		exitWithUsage()
	}

	switch benchType {
	case "pressure":
		pressuredBenchmarkClient(commandWrite)
	case "echo":
		echoBenchmarkClient(commandWrite)
	default:
		fmt.Printf("unknown type: %s\n", benchType)
		exitWithUsage()
	}
}

func pressuredBenchmarkClient(write bool) {
	// dial the remote address
	c, err := net.Dial(network, raddr)
	if err != nil {
		slog.Error(fmt.Sprintf("failed to dial %s: %v\n", raddr, err))
		return
	}

	wg := new(sync.WaitGroup)
	wg.Add(1)
	go func() {
		defer c.Close()
		defer wg.Done()

		pb := &benchmarkconn.PressuredBenchmark{
			MessageSize:   messageSize,
			TotalMessages: uint64(totalMessage),
		}

		if write {
			pb.Writer(c)
		} else {
			pb.Reader(c)
		}

		slog.Info(fmt.Sprintf("PressuredBenchmark Result: %v", pb.Result()))
	}()

	go func() {
		<-time.After(timeout)
		slog.Warn("timed out, closing the connection")
		c.Close()
	}()

	wg.Wait()
}

func echoBenchmarkClient(write bool) {
	// dial the remote address
	c, err := net.Dial(network, raddr)
	if err != nil {
		slog.Error(fmt.Sprintf("failed to dial %s: %v\n", raddr, err))
		return
	}

	wg := new(sync.WaitGroup)
	wg.Add(1)
	go func() {
		defer c.Close()
		defer wg.Done()

		ib := &benchmarkconn.IntervalBenchmark{
			MessageSize:   messageSize,
			TotalMessages: uint64(totalMessage),
			Interval:      interval,
			Echo:          true,
		}

		if write {
			if err := ib.Writer(c); err != nil {
				slog.Error(fmt.Sprintf("(*IntervalBenchmark).Writer: %v", err))
				return
			}
		} else {
			if err := ib.Reader(c); err != nil {
				slog.Error(fmt.Sprintf("(*IntervalBenchmark).Reader: %v", err))
				return
			}
		}

		slog.Info(fmt.Sprintf("EchoBenchmark Result: %v", ib.Result()))
	}()

	go func() {
		<-time.After(timeout)
		slog.Warn("timed out, closing the connection")
		c.Close()
	}()

	wg.Wait()
}
