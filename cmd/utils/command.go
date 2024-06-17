package utils

import (
	"flag"
	"fmt"
	"log/slog"
	"net"
	"sync"
	"time"

	"github.com/gaukas/benchmarkconn"
)

const (
	defaultNetwork = "tcp"
)

func NewBenchmark() *Benchmark {
	b := &Benchmark{
		fs: flag.NewFlagSet("", flag.ContinueOnError),
	}

	b.network = b.fs.String("net", defaultNetwork, "network type (tcp, udp, etc)")
	b.messageSz = b.fs.Int("sz", 1024, "size of the message to send/expect")
	b.totalMsg = b.fs.Int("m", 1000, "total number of messages to send/expect")
	b.interval = b.fs.Duration("i", 1*time.Millisecond, "minimal interval between each message, only for echo")
	b.timeout = b.fs.Duration("t", 10*time.Second, "timeout for the benchmark")

	return b
}

type Benchmark struct {
	fs *flag.FlagSet

	addr      string
	benchType string
	command   string

	network *string

	messageSz *int
	totalMsg  *int

	interval *time.Duration
	timeout  *time.Duration
}

func (b *Benchmark) Address() string {
	return b.addr
}

func (b *Benchmark) BenchType() string {
	return b.benchType
}

func (b *Benchmark) Command() string {
	return b.command
}

func (b *Benchmark) Usage() {
	fmt.Println("Example: <client|server> <type> <operation> <server_addr> [arguments...]")
	fmt.Printf("- Possible <type>: pressure, echo\n")
	fmt.Printf("- Possible <operation>: write, read\n\n")
	b.fs.Usage()
}

func (b *Benchmark) SetBenchType(benchType string) {
	b.benchType = benchType
}

func (b *Benchmark) SetCommand(command string) {
	b.command = command
}

func (b *Benchmark) SetAddress(addr string) {
	b.addr = addr
}

func (b *Benchmark) Init(args []string) error {
	if err := b.fs.Parse(args); err != nil {
		return err
	}

	return nil
}

func (b *Benchmark) Client() error {
	var writeBench bool
	switch b.command {
	case "write":
		writeBench = true
	case "read":
		writeBench = false
	default:
		b.Usage()
		return nil
	}

	switch b.benchType {
	case "pressure":
		b.pressuredBenchmarkClient(writeBench)
	case "echo":
		b.echoBenchmarkClient(writeBench)
	default:
		b.Usage()
		return nil
	}

	return nil
}

func (b *Benchmark) Server() error {
	var writeBench bool
	switch b.command {
	case "write":
		writeBench = true
	case "read":
		writeBench = false
	default:
		b.Usage()
		return nil
	}

	switch b.benchType {
	case "pressure":
		b.pressuredBenchmarkServer(writeBench)
	case "echo":
		b.echoBenchmarkServer(writeBench)
	default:
		b.Usage()
		return nil
	}

	return nil
}

func (b *Benchmark) pressuredBenchmarkClient(write bool) {
	// dial the remote address
	c, err := net.Dial(*b.network, b.addr)
	if err != nil {
		slog.Error(fmt.Sprintf("failed to dial %s: %v\n", b.addr, err))
		return
	}

	wg := new(sync.WaitGroup)
	wg.Add(1)
	go func() {
		defer c.Close()
		defer wg.Done()

		pb := &benchmarkconn.PressuredBenchmark{
			MessageSize:   *b.messageSz,
			TotalMessages: uint64(*b.totalMsg),
		}

		if write {
			pb.Writer(c)
		} else {
			pb.Reader(c)
		}

		slog.Info(fmt.Sprintf("PressuredBenchmark Result: %v", pb.Result()))
	}()

	go func() {
		<-time.After(*b.timeout)
		slog.Warn("timed out, closing the connection")
		c.Close()
	}()

	wg.Wait()
}

func (b *Benchmark) echoBenchmarkClient(write bool) {
	// dial the remote address
	c, err := net.Dial(*b.network, b.addr)
	if err != nil {
		slog.Error(fmt.Sprintf("failed to dial %s: %v\n", b.addr, err))
		return
	}

	wg := new(sync.WaitGroup)
	wg.Add(1)
	go func() {
		defer c.Close()
		defer wg.Done()

		ib := &benchmarkconn.IntervalBenchmark{
			MessageSize:   *b.messageSz,
			TotalMessages: uint64(*b.totalMsg),
			Interval:      *b.interval,
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
		<-time.After(*b.timeout)
		slog.Warn("timed out, closing the connection")
		c.Close()
	}()

	wg.Wait()
}

func (b *Benchmark) pressuredBenchmarkServer(write bool) {
	// listen on the specified address
	l, err := net.Listen(*b.network, b.addr)
	if err != nil {
		slog.Error(fmt.Sprintf("failed to listen on %s: %v\n", b.addr, err))
		return
	}

	slog.Info(fmt.Sprintf("server started, listening on %s", l.Addr()))

	// accept only one connection and run the benchmark
	c, err := l.Accept()
	if err != nil {
		slog.Error(fmt.Sprintf("failed to accept connection: %v\n", err))
		return
	}

	// if TCPConn, set the NoDelay option
	if tcpConn, ok := c.(*net.TCPConn); ok {
		tcpConn.SetNoDelay(true)
	}

	wg := new(sync.WaitGroup)
	wg.Add(1)
	go func() {
		defer c.Close()
		defer wg.Done()

		pb := &benchmarkconn.PressuredBenchmark{
			MessageSize:   *b.messageSz,
			TotalMessages: uint64(*b.totalMsg),
		}

		if write {
			pb.Writer(c)
		} else {
			pb.Reader(c)
		}

		slog.Info(fmt.Sprintf("PressuredBenchmark Result: %v", pb.Result()))
	}()

	go func() {
		<-time.After(*b.timeout)
		slog.Warn("timed out, closing the connection")
		c.Close()
	}()

	wg.Wait()
}

func (b *Benchmark) echoBenchmarkServer(write bool) {
	// listen on the specified address
	l, err := net.Listen(*b.network, b.addr)
	if err != nil {
		slog.Error(fmt.Sprintf("failed to listen on %s: %v\n", b.addr, err))
		return
	}

	slog.Info(fmt.Sprintf("server started, listening on %s", l.Addr()))

	// accept only one connection and run the benchmark
	c, err := l.Accept()
	if err != nil {
		slog.Error(fmt.Sprintf("failed to accept connection: %v\n", err))
		return
	}

	// if TCPConn, set the NoDelay option
	if tcpConn, ok := c.(*net.TCPConn); ok {
		tcpConn.SetNoDelay(true)
	}

	wg := new(sync.WaitGroup)
	wg.Add(1)
	go func() {
		defer c.Close()
		defer wg.Done()

		ib := &benchmarkconn.IntervalBenchmark{
			MessageSize:   *b.messageSz,
			TotalMessages: uint64(*b.totalMsg),
			Interval:      *b.interval,
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
		<-time.After(*b.timeout)
		slog.Warn("timed out, closing the connection")
		c.Close()
	}()

	wg.Wait()
}
