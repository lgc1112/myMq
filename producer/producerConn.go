package producer

import (
	"bufio"
	"net"
)

type producerConn struct {
	addr string
	Reader *bufio.Reader
	Writer *bufio.Writer
}

func newConn(addr string)  (*producerConn, error){
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}
	c := &producerConn{
		addr: addr,
		Reader: bufio.NewReader(conn),
		Writer: bufio.NewWriter(conn),
	}
	return c, nil
}
