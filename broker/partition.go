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
	msgChan     chan *protocol.Message
	comsummerClientsLock sync.RWMutex
	comsummerClients map[string] *client //groupName -> *client
}

const (
	msgChanSize = 1000
)
func newPartition(name, addr string)  *partition{
	myLogger.Logger.Printf("newPartition %s : %s", name, addr)
	partition := &partition{
		name : name,
		addr : addr,
		msgChan : make(chan *protocol.Message, msgChanSize),
		comsummerClients: make(map[string] *client),
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
		response := &protocol.Server2Client{
			Key: protocol.Server2ClientKey_PushMsg,
			Msg: msg,
		}
		p.comsummerClientsLock.RLock()
		for grp, client := range p.comsummerClients{
			if client == nil{
				myLogger.Logger.Printf("group %s invalid", grp)
				continue
			}
			myLogger.Logger.Printf("send msg to group %s", grp)
			client.writeChan <- response
		}
		p.comsummerClientsLock.RUnlock()
	}
}
func (p *partition) addComsummerClient(client *client, groupName string) {
	myLogger.Logger.Printf("partition %s  addComsummerClient : %s", p.name, groupName)
	p.comsummerClientsLock.Lock()
	p.comsummerClients[groupName] = client
	p.comsummerClientsLock.Unlock()
}


func (p *partition) invalidComsummerClient(client *client, groupName string) {//使得client失效，因为已经被删除
	p.comsummerClientsLock.Lock()
	c, ok := p.comsummerClients[groupName]
	if !ok {
		myLogger.Logger.Print("invalidComsummerClient not exist : ", groupName)
	}else{
		if c.id == client.id{ //id不等的话可能已被替换了
			p.comsummerClients[groupName] = nil
			myLogger.Logger.Printf("invalidComsummerClient : %s", groupName)
		}
	}
	p.comsummerClientsLock.Unlock()
}

func (p *partition) deleteComsummerClient(client *client, groupName string) {
	p.comsummerClientsLock.Lock()
	c, ok := p.comsummerClients[groupName]
	if !ok {
		myLogger.Logger.Print("deleteComsummerClient not exist : ", groupName)
	}else{
		if c.id == client.id{ //id不等的话可能已被替换了
			delete(p.comsummerClients, groupName)
			myLogger.Logger.Printf("deleteComsummerClient : %s", groupName)
		}
	}
	p.comsummerClientsLock.Unlock()
	return
}


