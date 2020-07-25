package broker

import (
	"../mylib/myLogger"
	"log"
	"net"
	. "sync"
)

type tcpServer struct {
	broker *Broker
	addr string
	listener net.Listener//closee这个就可以关闭startTcpServer协程
}



func newTcpServer(broker *Broker, addr string) *tcpServer{
	return &tcpServer{broker:broker, addr:addr}
}

func (t *tcpServer)startTcpServer() {
	var err error
	t.listener, err = net.Listen("tcp", t.addr)
	myLogger.Logger.Print("startTcpServer  " + t.addr)
	if err != nil {
		myLogger.Logger.Print(err)
		log.Fatal(err)
	}
	var wg WaitGroup
	for {
		conn, err := t.listener.Accept()
		if err != nil {
			myLogger.Logger.Print(err)
			break //一般是listener关闭了,则退出这个协程
		}
		myLogger.Logger.Print("new client " + conn.RemoteAddr().String())
		client := newClient(conn, t.broker)
		wg.Add(1)
		go func() {
			client.clientHandle()
			wg.Done()
		}()
	}
	myLogger.Logger.Print("TCP: close 1")
	wg.Wait()//等待client协程关闭
	myLogger.Logger.Print("TCP: close 2")

}
