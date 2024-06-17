package benchmarkconn

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"time"

	crand "crypto/rand"
)

type Benchmark interface {
	Writer(net.Conn, ...Counter) error
	Reader(net.Conn, ...Counter) error
	Result() map[string]any
}

// PressuredBenchmark is a benchmark that sends a fixed number of messages of a fixed size
// one after another as fast as possible and measures the throughput and latency.
type PressuredBenchmark struct {
	MessageSize   int    `json:"message_size" yaml:"message_size"`     // MessageSize defines how many bytes to write for each send attempt
	TotalMessages uint64 `json:"total_messages" yaml:"total_messages"` // TotalMessages defines how many messages to send in total

	messageSize      int // an internal copy of the message size used in the last run
	successfulReads  atomic.Uint64
	successfulWrites atomic.Uint64
	startTime        atomic.Value
	endTime          atomic.Value

	combinedCounter *CombinedCounter
}

func (b *PressuredBenchmark) Writer(conn net.Conn, counters ...Counter) error {
	// Compare benchmark specs on both sides
	specJson, err := json.Marshal(b)
	if err != nil {
		return err
	}

	specLenWr, err := conn.Write(specJson)
	if err != nil {
		return err
	}

	if specLenWr != len(specJson) {
		return errors.New("failed to write the spec to the connection")
	}

	receivedSpecJson := make([]byte, 2*len(specJson))
	specLenRd, err := conn.Read(receivedSpecJson)
	if err != nil {
		return err
	}

	if specLenRd != len(specJson) {
		return errors.New("failed to read the spec from the connection")
	}

	if !bytes.Equal(specJson, receivedSpecJson[:specLenRd]) {
		return errors.New("benchmark specs do not match, aborting")
	}

	// Create combined counter
	b.combinedCounter = CombineCounters(time.Second, counters...)

	// Benchmark starts
	b.messageSize = b.MessageSize
	b.successfulReads.Store(0)
	b.successfulWrites.Store(0)
	b.startTime.Store(time.Now())
	defer func() {
		b.endTime.Store(time.Now())
	}()

	// Start the counter
	if b.combinedCounter != nil {
		b.combinedCounter.Start()
		defer b.combinedCounter.Stop()
	}

	var randMsg = make([]byte, b.messageSize)
	var i uint64
	for i = 0; i < b.TotalMessages; i++ {
		crand.Read(randMsg)
		_, err := conn.Write(randMsg)
		if err != nil {
			return err
		}
		b.successfulWrites.Add(1)
	}

	return nil
}

func (b *PressuredBenchmark) Reader(conn net.Conn, counters ...Counter) error {
	// Compare benchmark specs on both sides
	specJson, err := json.Marshal(b)
	if err != nil {
		return err
	}

	receivedSpecJson := make([]byte, 2*len(specJson))
	specLenRd, err := conn.Read(receivedSpecJson)
	if err != nil {
		return err
	}

	if specLenRd != len(specJson) {
		return errors.New("failed to read the spec from the connection")
	}

	if !bytes.Equal(specJson, receivedSpecJson[:specLenRd]) {
		return errors.New("benchmark specs do not match, aborting")
	}

	specLenWr, err := conn.Write(specJson)
	if err != nil {
		return err
	}

	if specLenWr != len(specJson) {
		return errors.New("failed to write the spec to the connection")
	}

	// Create combined counter
	b.combinedCounter = CombineCounters(time.Second, counters...)

	// Benchmark starts
	b.messageSize = b.MessageSize
	b.successfulReads.Store(0)
	b.successfulWrites.Store(0)
	b.startTime.Store(time.Now())
	defer func() {
		b.endTime.Store(time.Now())
	}()

	// Start the counter
	if b.combinedCounter != nil {
		b.combinedCounter.Start()
		defer b.combinedCounter.Stop()
	}

	var receivedMsg = make([]byte, b.messageSize)
	for b.successfulReads.Load() < b.TotalMessages {
		// _, err := conn.Read(receivedMsg) // risk reading partial messages
		_, err := io.ReadFull(conn, receivedMsg) // read full length of the message
		if err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, net.ErrClosed) {
				return nil
			}
			return err
		}
		b.successfulReads.Add(1)
	}

	return nil
}

func (b *PressuredBenchmark) Result() map[string]any {
	if b.endTime.Load().(time.Time).IsZero() || b.endTime.Load().(time.Time).Sub(b.startTime.Load().(time.Time)).Nanoseconds() == 0 {
		return map[string]any{}
	}

	result := map[string]any{
		"successful_reads":  b.successfulReads.Load(),
		"successful_writes": b.successfulWrites.Load(),
		"start_time":        b.startTime.Load().(time.Time).Format(time.RFC3339),
		"end_time":          b.endTime.Load().(time.Time).Format(time.RFC3339),
		"duration":          b.endTime.Load().(time.Time).Sub(b.startTime.Load().(time.Time)).String(),
	}

	// Reader only: calculate ops_per_sec and latency_ms
	if b.successfulReads.Load() > 0 {
		result["ops_per_s"] = float64(b.successfulReads.Load()+b.successfulWrites.Load()) / float64(b.endTime.Load().(time.Time).Sub(b.startTime.Load().(time.Time)).Nanoseconds()) * 1e9
		result["latency_ns"] = float64(b.endTime.Load().(time.Time).Sub(b.startTime.Load().(time.Time)).Nanoseconds()) / float64(b.successfulReads.Load()+b.successfulWrites.Load()) // in nanoseconds
	}

	if b.combinedCounter != nil {
		result["counters"] = b.combinedCounter.Results()
	}

	return result
}

// IntervalBenchmark is a benchmark that sends a fixed number of messages of a fixed size
// one after another with a fixed interval between each send attempt and measures the
// throughput and latency.
type IntervalBenchmark struct {
	MessageSize   int           `json:"message_size" yaml:"message_size"`     // MessageSize defines how many bytes to write for each send attempt
	TotalMessages uint64        `json:"total_messages" yaml:"total_messages"` // TotalMessages defines how many messages to send in total
	Interval      time.Duration `json:"interval" yaml:"interval"`             // Interval defines how long to wait between each send attempt. If this value is too low, it is possible that the actual interval will be much higher due to system limitations
	Echo          bool          `json:"echo" yaml:"echo"`                     // Echo defines whether the receiver should echo back the received message

	messageSize      int // an internal copy of the message size used in the last run
	successfulReads  atomic.Uint64
	successfulWrites atomic.Uint64
	startTime        atomic.Value
	endTime          atomic.Value

	echoMap                  *sync.Map     // used for sender to calculate latency
	totalLatency             atomic.Uint64 // used for sender to calculate latency
	totalMessagesWithLatency atomic.Uint64 // used for sender to calculate latency
	ticker                   *time.Ticker

	combinedCounter *CombinedCounter
}

func (b *IntervalBenchmark) Writer(conn net.Conn, counters ...Counter) error {
	// Compare benchmark specs on both sides
	{
		specJson, err := json.Marshal(b)
		if err != nil {
			return err
		}

		specLenWr, err := conn.Write(specJson)
		if err != nil {
			return err
		}

		if specLenWr != len(specJson) {
			return errors.New("failed to write the spec to the connection")
		}

		receivedSpecJson := make([]byte, 2*len(specJson))
		specLenRd, err := conn.Read(receivedSpecJson)
		if err != nil {
			return err
		}

		if specLenRd != len(specJson) {
			return errors.New("failed to read the spec from the connection")
		}

		if !bytes.Equal(specJson, receivedSpecJson[:specLenRd]) {
			return errors.New("benchmark specs do not match, aborting")
		}
	}

	var exitedDueToDeadline atomic.Bool

	// Create combined counter
	b.combinedCounter = CombineCounters(time.Second, counters...)

	// Benchmark starts
	b.messageSize = b.MessageSize
	b.successfulReads.Store(0)
	b.successfulWrites.Store(0)
	b.startTime.Store(time.Now())
	defer func() {
		if exitedDueToDeadline.Load() {
			b.endTime.Store(time.Now().Add(-1*time.Second - b.Interval)) // subtract 1 second and the interval to account for the deadline
		} else {
			b.endTime.Store(time.Now())
		}
	}()
	b.echoMap = new(sync.Map)

	// Start the counter
	if b.combinedCounter != nil {
		b.combinedCounter.Start()
		defer b.combinedCounter.Stop()
	}

	var wgEcho sync.WaitGroup
	if b.Echo { // if echo is enabled start a goroutine to read echoed messages
		wgEcho.Add(1)
		go func() {
			defer wgEcho.Done()
			var receivedMsg = make([]byte, b.messageSize)
			for {
				conn.SetReadDeadline(time.Now().Add(1 * time.Second).Add(b.Interval)) // set a deadline for reading echoed messages
				// n, err := conn.Read(receivedMsg) // risk reading partial messages
				n, err := io.ReadFull(conn, receivedMsg) // read full length of the message
				if err != nil {
					if errors.Is(err, os.ErrDeadlineExceeded) {
						exitedDueToDeadline.Store(true)
					}
					return
				}
				if sendTime, ok := b.echoMap.Load(string(receivedMsg[:n])); ok {
					b.totalMessagesWithLatency.Add(1)
					b.echoMap.CompareAndDelete(string(receivedMsg[:n]), sendTime)

					// calculate latency
					latency := time.Since(sendTime.(time.Time)).Nanoseconds()
					b.totalLatency.Add(uint64(latency))
				}
			}
		}()
	}

	// Start sending messages using ticker
	b.ticker = time.NewTicker(b.Interval)

	var i uint64
	for i = 0; i < b.TotalMessages; i++ {
		<-b.ticker.C // wait for the interval
		var randMsg []byte = make([]byte, b.messageSize)
		crand.Read(randMsg)

		if b.Echo { // if echo is enabled, record the message to the echo map
			sendTime := time.Now()
			b.echoMap.Store(string(randMsg), sendTime) // save key as hash of the message and value as the time it was sent
		}

		_, err := conn.Write(randMsg)
		if err != nil {
			return err
		}

		b.successfulWrites.Add(1)
	}
	b.ticker.Stop()

	wgEcho.Wait()

	return nil
}

func (b *IntervalBenchmark) Reader(conn net.Conn, counters ...Counter) error {
	// Compare benchmark specs on both sides
	{
		specJson, err := json.Marshal(b)
		if err != nil {
			return err
		}

		receivedSpecJson := make([]byte, 2*len(specJson))
		specLenRd, err := conn.Read(receivedSpecJson)
		if err != nil {
			return err
		}

		if specLenRd != len(specJson) {
			return errors.New("failed to read the spec from the connection")
		}

		if !bytes.Equal(specJson, receivedSpecJson[:specLenRd]) {
			return errors.New("benchmark specs do not match, aborting")
		}

		specLenWr, err := conn.Write(specJson)
		if err != nil {
			return err
		}

		if specLenWr != len(specJson) {
			return errors.New("failed to write the spec to the connection")
		}
	}

	// Create combined counter
	b.combinedCounter = CombineCounters(time.Second, counters...)

	// Benchmark starts
	b.messageSize = b.MessageSize
	b.successfulReads.Store(0)
	b.successfulWrites.Store(0)
	b.startTime.Store(time.Now())
	defer func() {
		b.endTime.Store(time.Now())
	}()

	// Start the counter
	if b.combinedCounter != nil {
		b.combinedCounter.Start()
		defer b.combinedCounter.Stop()
	}

	var receivedMsg = make([]byte, b.messageSize)
	for b.successfulReads.Load() < b.TotalMessages {
		// n, err := conn.Read(receivedMsg) // risk reading partial messages
		n, err := io.ReadFull(conn, receivedMsg) // read full length of the message
		if err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, net.ErrClosed) {
				return nil
			}
			return err
		}
		b.successfulReads.Add(1)

		if b.Echo { // if echo is enabled, echo back the received message
			_, err := conn.Write(receivedMsg[:n])
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func (b *IntervalBenchmark) Result() map[string]any {
	if b.endTime.Load().(time.Time).IsZero() || b.endTime.Load().(time.Time).Sub(b.startTime.Load().(time.Time)).Nanoseconds() == 0 {
		return map[string]any{}
	}

	result := map[string]any{
		"successful_reads":  b.successfulReads.Load(),
		"successful_writes": b.successfulWrites.Load(),
		"start_time":        b.startTime.Load().(time.Time).Format(time.RFC3339),
		"end_time":          b.endTime.Load().(time.Time).Format(time.RFC3339),
		"duration":          b.endTime.Load().(time.Time).Sub(b.startTime.Load().(time.Time)).String(),
	}

	// Reader only: calculate ops_per_sec and latency_ms
	if b.successfulReads.Load() > 0 {
		result["ops_per_s"] = float64(b.successfulReads.Load()+b.successfulWrites.Load()) / float64(b.endTime.Load().(time.Time).Sub(b.startTime.Load().(time.Time)).Nanoseconds()) * 1e9
	}

	if b.totalMessagesWithLatency.Load() > 0 {
		result["latency_ns"] = float64(b.totalLatency.Load()) / float64(b.totalMessagesWithLatency.Load()) // in nanoseconds
	}

	if b.combinedCounter != nil {
		result["counters"] = b.combinedCounter.Results()
	}

	return result
}
