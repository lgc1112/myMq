package broker

import (
	"../mylib/myLogger"
	"log"
	"net"
	. "sync"
)

type brokerTcpServer struct {
	broker *Broker
	addr string
	listener net.Listener//closee这个就可以关闭startTcpServer协程
}


func NewBrokerTcpServer(broker *Broker, addr string) *brokerTcpServer{
	return &brokerTcpServer{broker:broker, addr:addr}
}
func (b *brokerTcpServer)Close() {
	b.listener.Close()
}
func (b *brokerTcpServer)startTcpServer() {
	var err error
	b.listener, err = net.Listen("tcp", b.addr)
	myLogger.Logger.Print("brokerTcpServer  " + b.addr)
	if err != nil {
		myLogger.Logger.Print(err)
		log.Fatal(err)
	}
	var wg WaitGroup
	for {
		conn, err := b.listener.Accept()
		if err != nil {
			myLogger.Logger.Print(err)
			break //一般是listener关闭了,则退出这个协程
		}
		myLogger.Logger.Print("new client " + conn.RemoteAddr().String())
		client := NewController2BrokerConn(conn, b.broker)
		wg.Add(1)
		go func() {
			client.clientHandle()
			wg.Done()
		}()
	}
	myLogger.Logger.Print("brokerTcpServer: close 1")
	wg.Wait()//等待client协程关闭
	myLogger.Logger.Print("brokerTcpServer: close 2")

}
