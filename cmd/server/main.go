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
	defaultLaddr   = ":0"
)

var (
	network string
	laddr   string

	messageSize  int
	totalMessage int
	interval     time.Duration

	timeout time.Duration
)

func init() {
	fmt.Printf("init\n")

	flag.StringVar(&network, "network", defaultNetwork, "network type (tcp, unix, etc)")
	flag.StringVar(&network, "net", defaultNetwork, "network type (tcp, unix, etc) (shorthand)")
	flag.StringVar(&laddr, "laddr", defaultLaddr, "local address to listen on")
	flag.StringVar(&laddr, "a", defaultLaddr, "local address to listen on (shorthand)")

	flag.IntVar(&messageSize, "message-size", 1024, "size of the message to send/expect")
	flag.IntVar(&messageSize, "sz", 1024, "size of the message to send/expect (shorthand)")
	flag.IntVar(&totalMessage, "total-message", 1000, "total number of messages to send/expect")
	flag.IntVar(&totalMessage, "m", 1000, "total number of messages to send/expect (shorthand)")
	flag.DurationVar(&interval, "interval", 1*time.Millisecond, "minimal interval between each message, ignored for type other than echo")
	flag.DurationVar(&interval, "i", 1*time.Millisecond, "minimal interval between each message, ignored for type other than echo (shorthand)")

	flag.DurationVar(&timeout, "timeout", 10*time.Second, "timeout for the server to wait for the benchmark to finish")
	flag.DurationVar(&timeout, "t", 10*time.Second, "timeout for the server to wait for the benchmark to finish (shorthand)")
}

func exitWithUsage() {
	flag.Usage()
	fmt.Println("To start the benchmark server: server type command [arguments...]")
	fmt.Printf("Possible type: pressure, echo\n")
	fmt.Printf("Possible commands: write, read\n")
	os.Exit(1)
}

func main() {
	flag.Parse()

	if flag.NArg() != 2 {
		exitWithUsage()
	}

	if laddr == "" {
		fmt.Println("local address is required")
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
		exitWithUsage()
	}

	switch benchType {
	case "pressure":
		pressuredBenchmarkServer(commandWrite)
	case "echo":
		echoBenchmarkServer(commandWrite)
	default:
		exitWithUsage()
	}
}

func pressuredBenchmarkServer(write bool) {
	// listen on the specified address
	l, err := net.Listen(network, laddr)
	if err != nil {
		slog.Error(fmt.Sprintf("failed to listen on %s: %v\n", laddr, err))
		return
	}

	slog.Info(fmt.Sprintf("server started, listening on %s", l.Addr()))

	// accept only one connection and run the benchmark
	c, err := l.Accept()
	if err != nil {
		slog.Error(fmt.Sprintf("failed to accept connection: %v\n", err))
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

func echoBenchmarkServer(write bool) {
	// listen on the specified address
	l, err := net.Listen(network, laddr)
	if err != nil {
		slog.Error(fmt.Sprintf("failed to listen on %s: %v\n", laddr, err))
		return
	}

	slog.Info(fmt.Sprintf("server started, listening on %s", l.Addr()))

	// accept only one connection and run the benchmark
	c, err := l.Accept()
	if err != nil {
		slog.Error(fmt.Sprintf("failed to accept connection: %v\n", err))
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
