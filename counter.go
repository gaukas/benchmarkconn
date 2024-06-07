package benchmarkconn

import (
	"sync"
	"time"
)

type CounterReport interface {
	Add(time time.Time, value any)
	Result() map[time.Time]any
}

type counterReport struct {
	internalMap *sync.Map
}

func NewCounterReport() CounterReport {
	return &counterReport{
		internalMap: new(sync.Map),
	}
}

func (r *counterReport) Add(time time.Time, value any) {
	r.internalMap.Store(time, value)
}

func (r *counterReport) Result() (result map[time.Time]any) {
	result = make(map[time.Time]any)
	r.internalMap.Range(func(key, value interface{}) bool {
		result[key.(time.Time)] = value.(int64)
		return true
	})
	return
}

type Counter interface {
	CountNow() // CountNow forcibly make the counter take a measurement immediately and save it to the result

	Start()
	Stop()
	Result() map[time.Time]any // Result values must be printable and/or JSON-serializable
}

type CombinedCounter struct {
	counters []Counter

	ticker   *time.Ticker
	interval time.Duration
	closed   chan bool
}

func CombineCounters(interval time.Duration, counters ...Counter) *CombinedCounter {
	report := make([]CounterReport, len(counters))
	for i := range report {
		report[i] = NewCounterReport()
	}
	return &CombinedCounter{
		counters: counters,
		interval: interval,
		closed:   make(chan bool),
	}
}

func (c *CombinedCounter) CountAllNow() {
	for _, counter := range c.counters {
		go counter.CountNow()
	}
}

func (c *CombinedCounter) Start() {
	c.ticker = time.NewTicker(c.interval)
	go func() {
		for {
			select {
			case <-c.ticker.C:
				c.CountAllNow()
			case <-c.closed:
				return
			}
		}
	}()
}

func (c *CombinedCounter) Stop() {
	c.ticker.Stop()
	close(c.closed)
}

func (c *CombinedCounter) Results() []map[time.Time]any {
	results := make([]map[time.Time]any, len(c.counters))
	for i, counter := range c.counters {
		results[i] = counter.Result()
	}
	return results
}

// CounterBase is an incomplete implementation of Counter.
type CounterBase struct {
	ticker   *time.Ticker
	interval time.Duration
	closed   chan bool
	report   CounterReport
}

// NewCounterBase creates a new CounterBase and initializes
// its internal fields.
func NewCounterBase(interval time.Duration) *CounterBase {
	return &CounterBase{
		interval: interval,
		closed:   make(chan bool),
		report:   NewCounterReport(),
	}
}

// Start starts the ticker only. Implementation
// of Counter must listen for the ticker and
// the closed channel.
func (c *CounterBase) Start() {
	c.ticker = time.NewTicker(c.interval)
}

// Stop stops the ticker and closes the closed channel.
//
// It is recommended for Counter implementations to directly
// inherit this method.
func (c *CounterBase) Stop() {
	c.ticker.Stop()
	close(c.closed)
}

// Result returns the result of the counter.
//
// It is recommended for Counter implementations to directly
// inherit this method.
func (c *CounterBase) Result() map[time.Time]any {
	return c.report.Result()
}

type cpuUsageCounter struct {
	*CounterBase
}

func NewCpuUsageCounter(interval time.Duration) Counter {
	return &cpuUsageCounter{
		CounterBase: NewCounterBase(interval),
	}
}

func (c *cpuUsageCounter) CountNow() {
	// c.report.Add(time.Now(), 0) // TODO: count CPU usage
}

func (c *cpuUsageCounter) Start() {
	c.CounterBase.Start()
	go func() {
		for {
			select {
			case <-c.ticker.C:
				c.CountNow()
			case <-c.closed:
				return
			}
		}
	}()
}

type memoryUsageCounter struct {
	*CounterBase
}

func NewMemoryUsageCounter(interval time.Duration) Counter {
	return &memoryUsageCounter{
		CounterBase: NewCounterBase(interval),
	}
}

func (c *memoryUsageCounter) CountNow() {
	// c.report.Add(time.Now(), 0) // TODO: count memory usage
}

func (c *memoryUsageCounter) Start() {
	c.CounterBase.Start()
	go func() {
		for {
			select {
			case <-c.ticker.C:
				c.CountNow()
			case <-c.closed:
				return
			}
		}
	}()
}
