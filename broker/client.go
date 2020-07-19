package broker

import (
	"bufio"
	"net"
)

type client struct {
	Reader *bufio.Reader
	Writer *bufio.Writer
}

func newClient(conn net.Conn)  *client{
	c := &client{
		Reader: bufio.NewReader(conn),
		Writer: bufio.NewWriter(conn),
	}
	return c;
}
