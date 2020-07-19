package broker

import (
	"../mylib/myLogger"
	"flag"
	"net"
	"sync"
)

type Broker struct {
	sync.RWMutex
	maxClientId int64
	addr string
	loger *myLogger.MyLogger
	tcpServer     *tcpServer
	topicMapLock sync.RWMutex
	topicMap map[string] *topic
	groupMapLock sync.RWMutex
	groupMap map[string] *group
	clientLock sync.RWMutex
	clientMap map[int64] *client
	tcpListener   net.Listener
	wg sync.WaitGroup
}
const logDir string = "./broker/log/"
func New() (*Broker, error) {

	addr := flag.String("addr", "localhost:12345", "ip:port")
	flag.Parse() //解析参数

	loger, err:= myLogger.New(logDir)

	broker := &Broker{
		loger:loger,
		topicMap: make(map[string]*topic),
		groupMap: make(map[string]*group),
		clientMap: make(map[int64]*client),
	}
	tcpServer := newTcpServer(broker, *addr)
	broker.tcpServer = tcpServer

	return broker, err;

}


func (b *Broker) Main() error {
	b.wg.Add(1)
	go func() {
		b.tcpServer.startTcpServer();
		b.wg.Done();
	}()
	b.wg.Wait()
	return nil
}

func (b *Broker) getTopic(topicName *string) (*topic, bool) {
	b.topicMapLock.RLock()
	topic, ok := b.topicMap[*topicName]
	b.topicMapLock.RUnlock();
	return topic, ok
}

func (b *Broker) addTopic(topicName *string, topic *topic) {
	b.topicMapLock.Lock()
	b.topicMap[*topicName] = topic
	b.topicMapLock.Unlock();
	return
}

func (b *Broker) getGroup(groupName *string) (*group, bool) {
	b.groupMapLock.RLock()
	group, ok := b.groupMap[*groupName]
	b.groupMapLock.RUnlock();
	return group, ok
}

func (b *Broker) addGroup(groupName *string, group *group) {
	b.groupMapLock.Lock()
	b.groupMap[*groupName] = group
	b.groupMapLock.Unlock();
	return
}

func (b *Broker) addClient(client *client) {
	b.clientLock.Lock()
	b.clientMap[client.id] = client
	b.clientLock.Unlock();
	return
}

func (b *Broker) removeClient(clientID int64) {
	b.clientLock.Lock()
	_, ok := b.clientMap[clientID]
	if !ok {
		b.clientLock.Unlock()
		return
	}
	delete(b.clientMap, clientID)
	b.clientLock.Unlock()
}