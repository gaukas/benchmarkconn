package benchmarkconn_test

import (
	"net"
	"sync"
	"testing"
	"time"

	. "github.com/gaukas/benchmarkconn"
)

func TestPressuredBenchmark(t *testing.T) {
	var senderPressuredBenchmark = &PressuredBenchmark{
		MessageSize:   1024,
		TotalMessages: 100000,
	}

	var receiverPressuredBenchmark = &PressuredBenchmark{
		MessageSize:   1024,
		TotalMessages: 100000,
	}

	tcpListener, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		t.Fatal(err)
	}

	senderConn, err := net.Dial("tcp", tcpListener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}

	receiverConn, err := tcpListener.Accept()
	if err != nil {
		t.Fatal(err)
	}

	senderConn.(*net.TCPConn).SetNoDelay(true)
	receiverConn.(*net.TCPConn).SetNoDelay(true)

	var wg sync.WaitGroup
	wg.Add(2)

	// Sender
	go func() {
		defer wg.Done()
		err := senderPressuredBenchmark.Writer(senderConn)
		if err != nil {
			t.Logf("Sender errored: %v", err)
		}
	}()

	// Receiver
	go func() {
		defer wg.Done()
		err := receiverPressuredBenchmark.Reader(receiverConn)
		if err != nil {
			t.Logf("Receiver errored: %v", err)
		}
	}()

	wg.Wait()

	t.Logf("Sender(%s): %v", senderConn.LocalAddr(), senderPressuredBenchmark.Result())
	t.Logf("Receiver(%s): %v", receiverConn.LocalAddr(), receiverPressuredBenchmark.Result())
}

func TestIntervalBenchmark(t *testing.T) {
	var senderIntervalBenchmark = &IntervalBenchmark{
		MessageSize:   1024,
		TotalMessages: 100000,
		Interval:      10 * time.Microsecond,
		Echo:          true,
	}

	var receiverIntervalBenchmark = &IntervalBenchmark{
		MessageSize:   1024,
		TotalMessages: 100000,
		Interval:      10 * time.Microsecond,
		Echo:          true,
	}

	tcpListener, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		t.Fatal(err)
	}

	senderConn, err := net.Dial("tcp", tcpListener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}

	receiverConn, err := tcpListener.Accept()
	if err != nil {
		t.Fatal(err)
	}

	senderConn.(*net.TCPConn).SetNoDelay(true)
	receiverConn.(*net.TCPConn).SetNoDelay(true)

	var wg sync.WaitGroup
	wg.Add(2)

	// Sender
	go func() {
		defer wg.Done()
		err := senderIntervalBenchmark.Writer(senderConn)
		if err != nil {
			t.Logf("Sender errored: %v", err)
		}
	}()

	// Receiver
	go func() {
		defer wg.Done()
		err := receiverIntervalBenchmark.Reader(receiverConn)
		if err != nil {
			t.Logf("Receiver errored: %v", err)
		}
	}()

	wg.Wait()

	t.Logf("Sender(%s): %v", senderConn.LocalAddr(), senderIntervalBenchmark.Result())
	t.Logf("Receiver(%s): %v", receiverConn.LocalAddr(), receiverIntervalBenchmark.Result())
}
