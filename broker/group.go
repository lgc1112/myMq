package broker

import (
	"../mylib/myLogger"
	"../mylib/protocalFuc"
	"../protocol"
	"github.com/golang/protobuf/proto"
	"sync"
	"sync/atomic"
	"time"
)

type group struct {
	name string
	broker *Broker
	sync.RWMutex
	rebalanceID int32//用于判断是不是最新的修改
	subscribedTopicsLock sync.RWMutex
	subscribedTopics [] *topic
	//subscribedTopics map[string] struct{} //key为订阅的topic
	clientsLock sync.RWMutex
	clients []*client
	client2PartitionMapLock sync.RWMutex
	client2PartitionMap map[int64] []*protocol.Partition //每个client被分配到的分区，clientId -> Partition
}

func newGroup(name string, rebalanceID int32, broker *Broker) *group {
	return &group{
		name: name,
		rebalanceID: rebalanceID,
		broker: broker,
		client2PartitionMap: make(map[int64] []*protocol.Partition),
	}
}

func (g *group)getClientPartition(clientId int64)  []*protocol.Partition{
	g.clientsLock.RLock()
	if containClient(g.clients, clientId){
		myLogger.Logger.Print("get Client exist")
		defer g.clientsLock.RUnlock()
		return g.client2PartitionMap[clientId]
	}
	g.clientsLock.RUnlock()
	myLogger.Logger.Print("get Client not exist")
	return nil
}

func (g *group)getSubscribedTopics() []string{
	var topics []string
	g.subscribedTopicsLock.RLock()
	for _, topic := range g.subscribedTopics {
		topics = append(topics, topic.name)
	}
	g.subscribedTopicsLock.RUnlock()
	return topics
}

func (g *group)addClient(client *client)  bool{
	//g.clientsLock.RLock()
	g.clientsLock.Lock()
	if containClient(g.clients, client.id){
		myLogger.Logger.Print("addClient exist")
		//g.clientsLock.RUnlock()
		g.clientsLock.Unlock()
		return false
	}
	//g.clientsLock.RUnlock()
	g.clients = append(g.clients, client)
	myLogger.Logger.Print("addClient success, num: ", len(g.clients))
	g.clientsLock.Unlock()
	return true
}
func containClient(items []*client, item int64) bool {
	for _, eachItem := range items {
		if eachItem.id == item {
			return true
		}
	}
	return false
}

func (g *group) deleteClient(clientId int64) (succ bool){
	myLogger.Logger.Print("deleteClient...")

	g.clientsLock.Lock()
	if !containClient(g.clients, clientId){
		myLogger.Logger.Print("deleteClient not exist")
		g.clientsLock.Unlock()
		return false
	}
	j := 0
	for _, client := range g.clients {
		if client.id != clientId {
			if g.clients[j].id != client.id{
				g.clients[j] = client
			}
			j++
		}
	}
	g.clients =  g.clients[:j]
	myLogger.Logger.Print("delete group Client success, group remain len:", len(g.clients))
	g.clientsLock.Unlock()
	return true
}

func (g *group)addTopic(topic *topic)( bool ){
	if containTopic(g.subscribedTopics, topic){
		myLogger.Logger.Print("group topic exist")
		return false
	}
	g.subscribedTopicsLock.Lock()
	g.subscribedTopics = append(g.subscribedTopics, topic)
	g.subscribedTopicsLock.Unlock()
	myLogger.Logger.Print("group addTopic success")
	return true
}
func containTopic(items []*topic, item *topic) bool {
	for _, eachItem := range items {
		if eachItem == item {
			return true
		}
	}
	return false
}
func (g *group)deleteTopic(topic *topic)  {
	if !containTopic(g.subscribedTopics, topic){
		return
	}
	g.subscribedTopicsLock.Lock()
	j := 0
	for _, val := range g.subscribedTopics {
		if val != topic {
			if g.subscribedTopics[j] != val{
				g.subscribedTopics[j] = val
			}
			j++
		}
	}
	g.subscribedTopics =  g.subscribedTopics[:j]
	g.subscribedTopicsLock.Unlock()
	//g.Rebalance()
}

func (g *group)notifyClients()  {
	myLogger.Logger.Print("notifyClients ")
	g.clientsLock.Lock()
	defer g.clientsLock.Unlock()
	for _, client := range g.clients{

		req := &protocol.ChangeConsumerPartitionReq{
			Partitions: g.client2PartitionMap[client.id],
			RebalanceId: g.rebalanceID,
		}
		data, err := proto.Marshal(req)
		if err != nil {
			myLogger.Logger.PrintError("marshaling error", err)
			return
		}
		reqData, err := protocalFuc.PackClientServerProtoBuf(protocol.ClientServerCmd_CmdChangeConsumerPartitionReq, data)
		if err != nil {
			myLogger.Logger.PrintError("PackClientServerProtoBuf error", err)
			return
		}

		writeCmdChan := client.getWriteCmdChan()
		select {
		case writeCmdChan <- reqData:
			//myLogger.Logger.Print("do not have client")
		case <-time.After(100 * time.Microsecond):
			myLogger.Logger.PrintWarning("notifyClient fail")
		}
		//tmp := &protocol.Server2Client{
		//	Key: protocol.Server2ClientKey_ChangeConsumerPartition,
		//	Partitions: g.client2PartitionMap[client.id],
		//	RebalanceId: g.rebalanceID,
		//}
		//response, err := proto.Marshal(tmp)
		//if err != nil {
		//	myLogger.Logger.PrintError("marshaling error: ", err)
		//	return
		//}
		//writeCmdChan := client.getWriteCmdChan()
		//select {
		//case writeCmdChan <- response:
		//	//myLogger.Logger.Print("do not have client")
		//case <-time.After(100 * time.Microsecond):
		//	myLogger.Logger.PrintWarning("notifyClient fail")
		//	return
		//}

	}
	myLogger.Logger.Print("notifyClients end")
}

//func (g *group)notifyClients()  {
//	myLogger.Logger.Print("notifyClients ")
//	g.clientsLock.Lock()
//	defer g.clientsLock.Unlock()
//	for _, client := range g.clients{
//		tmp := &protocol.Server2Client{
//			Key: protocol.Server2ClientKey_ChangeConsumerPartition,
//			Partitions: g.client2PartitionMap[client.id],
//			RebalanceId: g.rebalanceID,
//		}
//		response, err := proto.Marshal(tmp)
//		if err != nil {
//			myLogger.Logger.PrintError("marshaling error: ", err)
//			return
//		}
//		select {
//		case client.writeCmdChan <- response:
//			//myLogger.Logger.Print("do not have client")
//		case <-time.After(100 * time.Microsecond):
//			myLogger.Logger.PrintWarning("notifyClient fail")
//			return
//		}
//
//	}
//	myLogger.Logger.Print("notifyClients end")
//}

func (g *group)Rebalance(){
	myLogger.Logger.Print("rebalance")
	k := 0
	clientNum := len(g.clients)
	if clientNum == 0{
		myLogger.Logger.Print("do not have client")
		return
	}
	tmpMap:= make(map[int64] []*protocol.Partition)
	g.subscribedTopicsLock.RLock()
	for _, topic := range g.subscribedTopics { //取出该消费者组订阅的所有topic
		topic.partitionMapLock.RLock()
		for _, partition := range topic.partitionMap {//取出所有topic的所有分区
			if !g.broker.IsbrokerAlive(&partition.addr){ //判断该分区所在节点是否还存活，不存活的分区跳过
				myLogger.Logger.Print("partition do not have alive:", partition.name, "addr:", partition.addr)
				continue
			}
			clientId := g.clients[k % clientNum].id
			k++
			tmpMap[clientId] = append(tmpMap[clientId], &protocol.Partition{Name: partition.name, Addr: partition.addr})
 		}
		topic.partitionMapLock.RUnlock()
	}
	g.subscribedTopicsLock.RUnlock()
	g.client2PartitionMap = tmpMap
	atomic.AddInt32(&g.rebalanceID, 1)//更新id
	g.notifyClients()
}