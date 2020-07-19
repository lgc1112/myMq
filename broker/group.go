package broker

import "sync"

type group struct {
	sync.RWMutex
	subscribedTopicMapLock sync.RWMutex
	subscribedTopic [] *topic
	clientLock sync.RWMutex
	clients []*client
	client2PartitionMapLock sync.RWMutex
	client2PartitionMap map[int64] []*partition
}

func (g *group) rebalance(){

}
//func
//func (g *group) getTopic(topicName *string) (*topic, bool) {
//	g.subscribedTopicMapLock.RLock()
//	topic, ok := g.subscribedTopicMap[*topicName]
//	g.subscribedTopicMapLock.RUnlock();
//	return topic, ok
//}
//
//func (g *group) addTopic(topicName *string, topic *topic) {
//	g.subscribedTopicMapLock.Lock()
//	g.subscribedTopicMap[*topicName] = topic
//	g.subscribedTopicMapLock.Unlock();
//	return
//}
//
//func (g *group) addClient(client *client) {
//	g.clientLock.Lock()
//	g.clientMap[client.id] = client
//	g.clientLock.Unlock();
//	return
//}
//
//func (g *group) removeClient(clientID int64) {
//	g.clientLock.Lock()
//	_, ok := g.clientMap[clientID]
//	if !ok {
//		g.clientLock.Unlock()
//		return
//	}
//	delete(g.clientMap, clientID)
//	g.clientLock.Unlock()
//}