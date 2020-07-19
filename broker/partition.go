package broker
import (
	"../mylib/myLogger"
	"../protocol"
	"sync"
)
type partition struct {
	sync.RWMutex
	name string
	addr string
	groups map[string] *partionGroup
	msgChan     chan *protocol.Message
	comsummerClientsLock sync.RWMutex
	comsummerClients []*client
}

const (
	msgChanSize = 1000
)
func newPartition(name, addr string)  *partition{
	myLogger.Logger.Printf("newPartition %s : %s", name, addr)
	partition := &partition{
		name : name,
		addr : addr,
		groups: make(map[string]*partionGroup),
		msgChan : make(chan *protocol.Message, msgChanSize),
	}
	go partition.readLoop()
	return partition
}
func (p *partition) readLoop()  {
	var msg *protocol.Message
	for{
		select {
		case msg = <-p.msgChan:

		}
		myLogger.Logger.Printf("Partition %s readMsg %s", p.name, string(msg.Msg))
		response := &protocol.Response{
			ResponseType: protocol.ResponseType_TopicNotExisted,
			Msg: msg,
		}
		p.comsummerClientsLock.RLock()
		for _, client := range p.comsummerClients{
			client.writeChan <- response
		}
		p.comsummerClientsLock.RUnlock()
	}
}
func (p *partition) addComsummerClient(client *client) {
	p.comsummerClientsLock.Lock()
	p.comsummerClients = append(p.comsummerClients, client)
	p.comsummerClientsLock.Unlock()
	return
}

func (p *partition) deleteComsummerClient(client *client) {
	p.comsummerClientsLock.Lock()
	j := 0
	for _, val := range p.comsummerClients {
		if val != client {
			p.comsummerClients[j] = val
			j++
		}
	}
	p.comsummerClients =  p.comsummerClients[:j]
	p.comsummerClientsLock.Unlock()
	return
}

