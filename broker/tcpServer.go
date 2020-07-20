package broker

import (
	"../mylib/myLogger"
	"log"
	"net"
)

type tcpServer struct {
	broker *Broker;
	addr string
}



func newTcpServer(broker *Broker, addr string) *tcpServer{
	return &tcpServer{broker:broker, addr:addr};
}

func (t *tcpServer)startTcpServer() {
	listener, err := net.Listen("tcp", t.addr)
	myLogger.Logger.Print("startTcpServer  " + t.addr);
	if err != nil {
		myLogger.Logger.Print(err);
		log.Fatal(err)
	}
	for {
		conn, err := listener.Accept()
		if err != nil {
			myLogger.Logger.Print(err);
			continue
		}
		myLogger.Logger.Print("new client " + conn.RemoteAddr().String());
		client := newClient(conn, t.broker);
		go client.clientHandle(conn) // handle one connection at a time
	}
}
