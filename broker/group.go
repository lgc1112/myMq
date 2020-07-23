package broker

import (
	"sync"
	"../mylib/myLogger"
	"../protocol"
)

type group struct {
	name string
	sync.RWMutex
	subscribedTopicsLock sync.RWMutex
	subscribedTopics [] *topic
	clientsLock sync.RWMutex
	clients []*client
	client2PartitionMapLock sync.RWMutex
	client2PartitionMap map[int64] []*protocol.Partition
}

func newGroup(name string) *group {
	return &group{
		name: name,
		client2PartitionMap: make(map[int64] []*protocol.Partition),
	}
}
func (g *group)getClientPartition(clientId int64)  []*protocol.Partition{
	//g.clientsLock.RLock()
	if containClient(g.clients, clientId){
		myLogger.Logger.Print("get Client exist")
		defer g.clientsLock.RUnlock()
		return g.client2PartitionMap[clientId]
	}
	//g.clientsLock.RUnlock()
	myLogger.Logger.Print("get Client not exist")
	return nil
}

func (g *group)addClient(client *client)  bool{
	//g.clientsLock.RLock()
	if containClient(g.clients, client.id){
		myLogger.Logger.Print("addClient exist")
		//g.clientsLock.RUnlock()
		return false
	}
	//g.clientsLock.RUnlock()
	//g.clientsLock.Lock()
	g.clients = append(g.clients, client)
	myLogger.Logger.Print("addClient success, num: ", len(g.clients))
	//g.clientsLock.Unlock()
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
	if !containClient(g.clients, clientId){
		myLogger.Logger.Print("deleteClient not exist")
		return false
	}
	g.clientsLock.Lock()
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
	g.rebalance()
}

func (g *group)notifyClients()  {
	myLogger.Logger.Print("notifyClients ")
	g.clientsLock.Lock()
	for _, client := range g.clients{
		response := &protocol.Server2Client{
			Key: protocol.Server2ClientKey_ChangeConsumerPartition,
			Partitions: g.client2PartitionMap[client.id],
		}
		client.writeChan <- response
		//client.sendResponse(response)
	}
	g.clientsLock.Unlock()
}

func (g *group)rebalance(){
	myLogger.Logger.Print("rebalance")
	k := 0
	clientNum := len(g.clients)
	if clientNum == 0{
		myLogger.Logger.Print("do not have client")
		return
	}
	tmpMap:= make(map[int64] []*protocol.Partition)
	for _, topic := range g.subscribedTopics {
		for _, partition := range topic.partitionMap {
			clientId := g.clients[k % clientNum].id
			k++
			tmpMap[clientId] = append(tmpMap[clientId], &protocol.Partition{Name: partition.name, TopicName: topic.name, Addr: partition.addr})
 		}
	}
	g.client2PartitionMap = tmpMap
	g.notifyClients()
}