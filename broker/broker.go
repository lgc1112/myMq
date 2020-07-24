package broker

import (
	"../mylib/myLogger"
	"../protocol"
	"flag"
	"fmt"
	"net"
	"os"
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

	clientMapLock sync.RWMutex
	clientMap map[int64] *client

	tcpListener   net.Listener
	wg sync.WaitGroup

	readChan     chan *readData
	clientChangeChan     chan *clientChange
}
type clientChange struct {
	isAdd bool
	client *client
	waitFinished  chan bool
}

type readData struct {
	clientID int64
	client2serverData *protocol.Client2Server
}

const logDir string = "./broker/log/"
func New() (*Broker, error) {
	addr := flag.String("addr", "0.0.0.0:12345", "ip:port")
	flag.Parse() //解析参数

	if *addr == "0.0.0.0:12345" {
		*addr = getIntranetIp() + ":12345"
	}

	_, err:= myLogger.New(logDir)

	broker := &Broker{
		topicMap: make(map[string]*topic),
		groupMap: make(map[string]*group),
		clientMap: make(map[int64]*client),
		partitionMap: make(map[string]*partition),
		readChan: make(chan *readData),
		clientChangeChan: make(chan *clientChange),
		addr: *addr,
	}
	tcpServer := newTcpServer(broker, *addr)
	broker.tcpServer = tcpServer

	return broker, err

}

func getIntranetIp() string{
	addrs, err := net.InterfaceAddrs()

	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	for _, address := range addrs {
		if ipnet, ok := address.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
			if ipnet.IP.To4() != nil {
				return ipnet.IP.String()
			}

		}
	}
	return "0.0.0.0"
}
func (b *Broker) Main() error {
	b.wg.Add(2)
	go func() {
		b.ReadLoop()
		b.wg.Done()
	}()

	go func() {
		b.tcpServer.startTcpServer()
		b.wg.Done()
	}()
	b.wg.Wait()
	return nil
}

func (b *Broker) getTopic(topicName *string) (*topic, bool) {
	//b.topicMapLock.RLock()
	topic, ok := b.topicMap[*topicName]
	//b.topicMapLock.RUnlock()
	return topic, ok
}

func (b *Broker) addTopic(topicName *string, topic *topic) {
	//b.topicMapLock.Lock()
	b.topicMap[*topicName] = topic
	//b.topicMapLock.Unlock()
	return
}

func (b *Broker) getPartition(partitionName *string) (*partition, bool) {
	//b.partitionMapLock.RLock()
	partition, ok := b.partitionMap[*partitionName]
	//b.partitionMapLock.RUnlock()
	return partition, ok
}

func (b *Broker) addPartition(partitionName *string, partition *partition){
	//b.partitionMapLock.Lock()
	b.partitionMap[*partitionName] = partition
	//b.partitionMapLock.Unlock()
	return
}

func (b *Broker) getGroup(groupName *string) (*group, bool) {
	//b.groupMapLock.RLock()
	group, ok := b.groupMap[*groupName]
	//b.groupMapLock.RUnlock()
	return group, ok
}

func (b *Broker) addGroup(group *group) {
	//b.groupMapLock.Lock()
	b.groupMap[group.name] = group
	//b.groupMapLock.Unlock()
	return
}

func (b *Broker) getClient(clientId int64) (*client, bool) {
	//b.clientMapLock.RLock()
	client, ok := b.clientMap[clientId]
	//b.clientMapLock.RUnlock()
	return client, ok
}


func (b *Broker) addClient(client *client) {
	//b.clientMapLock.Lock()
	b.clientMap[client.id] = client
	myLogger.Logger.Print("addClient, pre len:", len(b.clientMap))
	//b.clientMapLock.Unlock()
	return
}

func (b *Broker) removeClient(clientId int64) {
	//b.clientMapLock.Lock()
	_, ok := b.clientMap[clientId]
	if !ok {
		myLogger.Logger.Print("remove broker Client not exist, remain len:", len(b.clientMap))
		b.clientMapLock.Unlock()
		return
	}
	delete(b.clientMap, clientId)
	myLogger.Logger.Print("remove broker Client success, remain len:", len(b.clientMap))
	//b.clientMapLock.Unlock()
}

func (b *Broker) ReadLoop() {
	var data *readData
	for{
		myLogger.Logger.Print("Broker readLoop")
		select {
		case tmp := <- b.clientChangeChan:
			if tmp.isAdd{
				b.addClient(tmp.client)
			}else{
				b.removeClient(tmp.client.id)
			}
			tmp.waitFinished <- true
			continue
		case data = <- b.readChan:

		}

		clientConn, ok := b.getClient(data.clientID)
		if !ok {
			myLogger.Logger.Print("client conn have close", data)
			continue
		}

		myLogger.Logger.Print("Broker read new data")


		var response *protocol.Server2Client
		switch data.client2serverData.Key {
		case protocol.Client2ServerKey_CreatTopic:
			response = b.creatTopic(data)
		case protocol.Client2ServerKey_GetPublisherPartition:
			response = b.getPublisherPartition(data)
		case protocol.Client2ServerKey_Publish:
			response = b.publish(data)
		case protocol.Client2ServerKey_GetConsumerPartition:
			response = b.getConsumerPartition(data)
		case protocol.Client2ServerKey_SubscribePartion:
			response = b.subscribePartition(data)
		case protocol.Client2ServerKey_SubscribeTopic:
			response = b.subscribeTopic(data)
		case protocol.Client2ServerKey_RegisterConsumer:
			response = b.registerConsumer(data)
		case protocol.Client2ServerKey_UnRegisterConsumer:
			response = b.unRegisterConsumer(data)
		case protocol.Client2ServerKey_ConsumeSuccess:
			response = b.consumeSuccess(data)
		default:
			myLogger.Logger.Print("cannot find key")
		}
		if response != nil{
			clientConn.writeCmdChan <- response
		}
		
		
	}
}


func (b *Broker)  creatTopic(data *readData)  (response *protocol.Server2Client) {
	request := data.client2serverData
	topicName := request.Topic
	partionNum := request.PartitionNum
	topic, ok := b.getTopic(&topicName)
	if ok {
		myLogger.Logger.Printf("try to create existed topic : %s %d", topicName, int(partionNum))
		response = &protocol.Server2Client{
			Key: protocol.Server2ClientKey_TopicExisted,
			Partitions: topic.getPartitions(),
		}
	} else {
		myLogger.Logger.Printf("create topic : %s %d", topicName, int(partionNum))
		topic = newTopic(topicName, int(partionNum), b)
		b.addTopic(&topicName, topic)
		response = &protocol.Server2Client{
			Key: protocol.Server2ClientKey_Success,
			Partitions: topic.getPartitions(),
		}
	}
	return response
}

func (b *Broker)  getPublisherPartition(data *readData)   (response *protocol.Server2Client) {
	request := data.client2serverData
	topicName := request.Topic
	topic, ok := b.getTopic(&topicName)
	if ok {
		myLogger.Logger.Printf("getPublisherPartition : %s", topicName)
		response = &protocol.Server2Client{
			Key: protocol.Server2ClientKey_Success,
			Partitions: topic.getPartitions(),
		}
	} else {
		myLogger.Logger.Printf("Partition Not existed : %s", topicName)
		response = &protocol.Server2Client{
			Key: protocol.Server2ClientKey_TopicNotExisted,
		}
	}
	return response
}


func (b *Broker)  consumeSuccess(data *readData)   (response *protocol.Server2Client) {
	request := data.client2serverData
	//topicName := request.Topic
	partitionName := request.Partition

	partition, ok := b.getPartition(&partitionName)
	if !ok {
		myLogger.Logger.Printf("Partition Not existed : %s", partitionName)
		response = &protocol.Server2Client{
			Key: protocol.Server2ClientKey_TopicNotExisted,
		}
		return response
	}else {
		//myLogger.Logger.Printf("publish msg : %s", msg.String())
		msgAskData := &msgAskData{
			msgId: request.MsgId,
			groupName: request.GroupName,
		}
		partition.msgAskChan <- msgAskData
		//response = &protocol.Server2Client{
		//	Key: protocol.Server2ClientKey_Success,
		//}
		return nil
	}
}

func (b *Broker)  publish(data *readData)   (response *protocol.Server2Client) {
	request := data.client2serverData
	//topicName := request.Topic
	partitionName := request.Partition
	msg := request.Msg

	partition, ok := b.getPartition(&partitionName)
	if !ok {
		myLogger.Logger.Printf("Partition Not existed : %s", partitionName)
		response = &protocol.Server2Client{
			Key: protocol.Server2ClientKey_TopicNotExisted,
		}
		return response
	}else{
		myLogger.Logger.Printf("publish msg : %s", msg.String())
		partition.msgChan <- msg
		response = &protocol.Server2Client{
			Key: protocol.Server2ClientKey_Success,
		}
		return response
	}
	//
	//topic, ok := b.getTopic(&topicName)
	//if !ok {
	//	myLogger.Logger.Printf("Topic Not existed : %s", topicName)
	//	response = &protocol.Server2Client{
	//		Key: protocol.Server2ClientKey_TopicNotExisted,
	//	}
	//	return response
	//}else{
	//	partition, ok := topic.getPartition(&partitionName)
	//	if !ok {
	//		myLogger.Logger.Printf("Partition Not existed : %s", partitionName)
	//		response = &protocol.Server2Client{
	//			Key: protocol.Server2ClientKey_TopicNotExisted,
	//		}
	//		return response
	//	}else{
	//		myLogger.Logger.Printf("publish msg : %s", msg.String())
	//		partition.msgChan <- msg
	//		response = &protocol.Server2Client{
	//			Key: protocol.Server2ClientKey_Success,
	//		}
	//		return response
	//	}
	//}

}
func (b *Broker)  getConsumerPartition(data *readData)   (response *protocol.Server2Client) {
	request := data.client2serverData
	groupName := request.GroupName
	group, ok := b.getGroup(&groupName)
	myLogger.Logger.Printf("registerComsummer : %s", groupName)
	if !ok {
		return nil
		//group = newGroup(groupName)
		//b.addGroup(group)
	}

	partitions := group.getClientPartition(data.clientID)
	if partitions == nil{//不存在
		return nil
	}
	response = &protocol.Server2Client{
		Key: protocol.Server2ClientKey_ChangeConsumerPartition,
		Partitions: partitions,
	}
	return response
}

func (b *Broker)  subscribeTopic(data *readData)   (response *protocol.Server2Client) {
	request := data.client2serverData
	topicName := request.Topic
	topic, ok := b.getTopic(&topicName)
	if !ok {
		myLogger.Logger.Printf("Topic Not existed : %s", topicName)
		response = &protocol.Server2Client{
			Key: protocol.Server2ClientKey_Error,
		}
		return response
	}

	groupName := request.GroupName
	group, ok := b.getGroup(&groupName)
	myLogger.Logger.Printf("registerComsummer : %s", groupName)
	if !ok {
		myLogger.Logger.Printf("newGroup : %s", groupName)
		group = newGroup(groupName)
		b.addGroup(group)
	}
	clientConn, ok := b.getClient(data.clientID)
	if !ok {
		myLogger.Logger.Print("clientConn have close")
	}

	clientConn.belongGroup = groupName
	succ:= group.addTopic(topic)
	succ = group.addClient(clientConn) || succ
	if succ{
		group.rebalance()
		return nil
	}else{
		response = &protocol.Server2Client{
			Key: protocol.Server2ClientKey_TopicExisted,
		}
		return response
	}
}


func (b *Broker)  subscribePartition(data *readData)   (response *protocol.Server2Client) {
	request := data.client2serverData
	//topicName := request.Topic
	partitionName := request.Partition
	groupName := request.GroupName
	partition, ok := b.getPartition(&partitionName)
	if !ok {
		myLogger.Logger.Printf("Partition Not existed : %s", partitionName)
		response = &protocol.Server2Client{
			Key: protocol.Server2ClientKey_Error,
		}
		return response
	}else{
		clientConn, ok := b.getClient(data.clientID)
		if !ok {
			myLogger.Logger.Print("clientConn have close")
		}
		clientConn.consumePartions[partitionName] = true
		partition.addComsummerClient(clientConn, groupName)
		response = &protocol.Server2Client{
			Key: protocol.Server2ClientKey_Success,
		}
		return response
	}

	//topic, ok := b.getTopic(&topicName)
	//if !ok {
	//	myLogger.Logger.Printf("Topic Not existed : %s", topicName)
	//	response = &protocol.Server2Client{
	//		Key: protocol.Server2ClientKey_TopicNotExisted,
	//	}
	//	return response
	//}else{
	//	partition, ok := topic.getPartition(&partitionName)
	//	if !ok {
	//		myLogger.Logger.Printf("Partition Not existed : %s", partitionName)
	//		response = &protocol.Server2Client{
	//			Key: protocol.Server2ClientKey_Error,
	//		}
	//		return response
	//	}else{
	//		clientConn, ok := b.getClient(data.clientID)
	//		if !ok {
	//			myLogger.Logger.Print("clientConn have close")
	//		}
	//		clientConn.consumePartions[partitionName] = true
	//		partition.addComsummerClient(clientConn, groupName)
	//		response = &protocol.Server2Client{
	//			Key: protocol.Server2ClientKey_Success,
	//		}
	//		return response
	//	}
	//}
}

func (b *Broker)  registerConsumer(data *readData)   (response *protocol.Server2Client) {
	request := data.client2serverData
	groupName := request.GroupName
	group, ok := b.getGroup(&groupName)
	myLogger.Logger.Printf("registerComsummer : %s", groupName)
	if !ok {
		group = newGroup(groupName)
		b.addGroup(group)
	}
	clientConn, ok := b.getClient(data.clientID)
	if !ok {
		myLogger.Logger.Print("clientConn have close")
	}
	succ := group.addClient(clientConn)
	if succ{//已通过go balance response
		return nil
	}
	response = &protocol.Server2Client{
		Key: protocol.Server2ClientKey_ChangeConsumerPartition,
		Partitions: group.getClientPartition(clientConn.id),
	}
	return response
}

func (b *Broker)  unRegisterConsumer(data *readData)   (response *protocol.Server2Client) {
	request := data.client2serverData
	groupName := request.GroupName
	group, ok := b.getGroup(&groupName)
	myLogger.Logger.Printf("registerComsummer : %s", groupName)
	if !ok {
		myLogger.Logger.Printf("group not exist : %s", groupName)
		return nil
	}
	group.deleteClient(data.clientID)
	return nil
}
