package broker

import (
	"../mylib/myLogger"
	"flag"
	"net"
	"sync"
)

type Broker struct {
	addr string
	loger *myLogger.MyLogger
	tcpServer     *tcpServer
	topicMap map[string] *topic
	tcpListener   net.Listener
	wg sync.WaitGroup
}
const logDir string = "./broker/log/"
func New() (*Broker, error) {

	addr := flag.String("addr", "localhost:12345", "ip:port")
	flag.Parse() //解析参数

	loger, err:= myLogger.New(logDir)

	broker := &Broker{loger:loger, topicMap:make(map[string] *topic) };
	tcpServer := newTcpServer(broker, *addr)
	broker.tcpServer = tcpServer

	return broker, err;

}


func (b *Broker) Main() error {
	go b.tcpServer.startTcpServer();

	exitCh := make(chan error)
	<-exitCh
	return nil
}