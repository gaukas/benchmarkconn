package benchmarkconn

import "net"

type Benchmark interface {
	Client(net.Conn, ...Counter) error
	Server(net.Conn, ...Counter) error
	Result() Result
}
