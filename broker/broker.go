package broker

import (
	"../mylib/myLogger"
	"flag"
	"net"
	"sync"
)

type Broker struct {
	maxClientId int64
	addr string
	tcpServer     *tcpServer

	topicMapLock sync.RWMutex
	topicMap map[string] *topic

	partitionMapLock sync.RWMutex
	partitionMap map[string] *partition

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

	_, err:= myLogger.New(logDir)

	broker := &Broker{
		topicMap: make(map[string]*topic),
		groupMap: make(map[string]*group),
		clientMap: make(map[int64]*client),
		partitionMap: make(map[string]*partition),
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
	b.topicMapLock.RUnlock()
	return topic, ok
}

func (b *Broker) addTopic(topicName *string, topic *topic) {
	b.topicMapLock.Lock()
	b.topicMap[*topicName] = topic
	b.topicMapLock.Unlock()
	return
}

func (b *Broker) getPartition(partitionName *string) (*partition, bool) {
	b.partitionMapLock.RLock()
	partition, ok := b.partitionMap[*partitionName]
	b.partitionMapLock.RUnlock()
	return partition, ok
}

func (b *Broker) addPartition(partitionName *string, partition *partition){
	b.partitionMapLock.Lock()
	b.partitionMap[*partitionName] = partition
	b.partitionMapLock.Unlock()
	return
}

func (b *Broker) getGroup(groupName *string) (*group, bool) {
	b.groupMapLock.RLock()
	group, ok := b.groupMap[*groupName]
	b.groupMapLock.RUnlock()
	return group, ok
}

func (b *Broker) addGroup(group *group) {
	b.groupMapLock.Lock()
	b.groupMap[group.name] = group
	b.groupMapLock.Unlock()
	return
}

func (b *Broker) addClient(client *client) {
	b.clientLock.Lock()
	b.clientMap[client.id] = client
	b.clientLock.Unlock()
	return
}

func (b *Broker) removeClient(client *client) {
	b.clientLock.Lock()
	_, ok := b.clientMap[client.id]
	if !ok {
		b.clientLock.Unlock()
		return
	}
	delete(b.clientMap, client.id)
	b.clientLock.Unlock()
}