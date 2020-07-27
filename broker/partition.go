package broker

import (
	"../mylib/myLogger"
	"../protocol"
	"math"
	"sync"
)
type partition struct {
	sync.RWMutex
	name string
	addr string
	subscribedGroupsLock sync.RWMutex
	subscribedGroups map[string] *subscribedGroup //groupName -> *client
	curMsgId int32

	msgChan     chan *protocol.Message
	msgAskChan     chan *msgAskData
	exitFinishedChan chan string
	exitChan chan string
}

const (
	msgChanSize = 0
)

type msgAskData struct {
	msgId int32
	groupName string
}
func newPartition(name, addr string)  *partition{
	myLogger.Logger.Printf("newPartition %s : %s", name, addr)
	partition := &partition{
		name : name,
		addr : addr,
		msgChan : make(chan *protocol.Message, msgChanSize),
		subscribedGroups: make(map[string] *subscribedGroup),
		exitFinishedChan: make(chan string),
		msgAskChan: make(chan *msgAskData),
		exitChan: make(chan string),
	}
	go partition.readLoop()
	return partition
}

func (p *partition) generateMsgId() int32{
	var nextId int32
	if p.curMsgId == math.MaxInt32{
		//atomic.StoreInt32(&p.curMsgId, 0)
		p.curMsgId = 0
		nextId = 0
	}else{
		//nextId = atomic.AddInt32(&p.curMsgId, 1)
		p.curMsgId++
		nextId = p.curMsgId
	}
	return nextId
}

func (p *partition) readLoop()  {
	var msg *protocol.Message
	for{
		select {
		case <- p.exitChan:
			goto exit
		case msgAskData := <- p.msgAskChan:
			p.subscribedGroupsLock.RLock()
			subscribedGroup, ok := p.subscribedGroups[msgAskData.groupName]
			if !ok {
				myLogger.Logger.Print("msgAskData group not exist :", msgAskData.groupName)
			}
			subscribedGroup.msgAskChan <- msgAskData.msgId
			p.subscribedGroupsLock.RUnlock()
		case msg = <- p.msgChan:
			nextId := p.generateMsgId()
			internalMsg := &protocol.InternalMessage{
				Id: nextId,
				Priority: msg.Priority,
				Msg: msg.Msg,
			}
			myLogger.Logger.Printf("Partition %s readMsg %s", p.name, string(msg.Msg))
			//response := &protocol.Server2Client{
			//	Key: protocol.Server2ClientKey_PushMsg,
			//	Msg: msg,
			//}
			p.subscribedGroupsLock.RLock()
			for grpName, subGrp := range p.subscribedGroups{
				myLogger.Logger.Printf("send msg to subscribedGroups %s", grpName)
				subGrp.readChan <- internalMsg
			}
			p.subscribedGroupsLock.RUnlock()
		}
	}
exit:
	myLogger.Logger.Printf("Partition %s exiting", p.name)
	p.exit()
}

func (p *partition) exit(){
	p.subscribedGroupsLock.Lock()
	for _, subscribedGroup:= range p.subscribedGroups{
		subscribedGroup.exit()
		//<- subscribedGroup.exitFinishedChan
	}
	myLogger.Logger.Printf("Partition %s exit finished", p.name)
	p.subscribedGroupsLock.Unlock()
	p.exitFinishedChan <- "bb"
	close(p.msgChan)
	close(p.msgAskChan)
	close(p.exitChan)
}

func (p *partition) addComsummerClient(client *client, groupName string) {
	myLogger.Logger.Printf("partition %s  addComsummerClient : %s", p.name, groupName)
	p.subscribedGroupsLock.Lock()
	subscribedGroup, ok:= p.subscribedGroups[groupName]

	if !ok {
		subscribedGroup = newPartionGroup(groupName, p.name)
		p.subscribedGroups[groupName] = subscribedGroup
	}

	waitFinished := make(chan bool)

	subscribedGroup.clientChangeChan <- &clientChange{true, client, waitFinished}//放到partitiongroup的readLoop协程中进行处理，避免频繁使用锁
	<- waitFinished //等待处理
	p.subscribedGroupsLock.Unlock()

	myLogger.Logger.Printf("addComsummerClient : %s", groupName)

	//subscribedGroup.consumerClientLock.Lock()
	//subscribedGroup.consumerClient = client
	//subscribedGroup.consumerClientLock.Unlock()
}


func (p *partition) invalidComsummerClient(client *client, groupName string) {//使得client失效，因为已经被删除
	p.subscribedGroupsLock.Lock()
	g, ok := p.subscribedGroups[groupName]
	if !ok {
		myLogger.Logger.Print("invalidComsummerClient not exist : ", groupName)
	}else{
		waitFinished := make(chan bool)
		g.clientChangeChan <- &clientChange{false, client, waitFinished}//放到partitiongroup的readLoop协程中进行处理，避免频繁使用锁
		<- waitFinished //等待处理
		myLogger.Logger.Printf("invalidComsummerClient : %s", groupName)

		//g.consumerClientLock.Lock()
		//if g.consumerClient.id == client.id{ //id不等的话可能已被替换了,不用处理
		//	p.subscribedGroups[groupName].consumerClient = nil
		//	myLogger.Logger.Printf("invalidComsummerClient : %s", groupName)
		//}
		//g.consumerClientLock.Unlock()
	}
	p.subscribedGroupsLock.Unlock()
}

func (p *partition) deleteComsummerClient(client *client, groupName string) {
	p.subscribedGroupsLock.Lock()
	g, ok := p.subscribedGroups[groupName]
	if !ok {
		myLogger.Logger.Print("deleteComsummerClient not exist : ", groupName)
	}else{

		waitFinished := make(chan bool)
		g.clientChangeChan <- &clientChange{false, client, waitFinished}//放到partitiongroup的readLoop协程中进行处理，避免频繁使用锁
		<- waitFinished //等待处理
		myLogger.Logger.Printf("invalidComsummerClient : %s", groupName)

		//g.consumerClientLock.Lock()
		//if g.consumerClient.id == client.id{ //id不等的话可能已被替换了
		//	delete(p.subscribedGroups, groupName)
		//	myLogger.Logger.Printf("deleteComsummerClient : %s", groupName)
		//}
		//g.consumerClientLock.Unlock()
	}
	p.subscribedGroupsLock.Unlock()
	return
}


