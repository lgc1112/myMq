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
	subscribedGroups map[string] *subscribedGroup //groupName -> *subscribedGroup
	curMsgId int32
	wg sync.WaitGroup

	broker *Broker
	msgChan     chan *protocol.Message
	responseChan chan *protocol.Server2Client
	msgAskChan     chan *msgAskData
	//exitFinishedChan chan string
	exitChan chan string

	isNativePartition bool //判断该分区是否在本地broker中
}

const (
	msgChanSize = 0
)

type msgAskData struct {
	msgId int32
	groupName string
}
func newPartition(name string, addr string, isNativePartiton bool, broker *Broker)  *partition{
	if !isNativePartiton{ //不是本地的分区
		partition := &partition{
			name : name,
			addr : addr,
			broker: broker,
			isNativePartition:isNativePartiton,
		}
		return partition
	}


	myLogger.Logger.Printf("newPartition %s : %s", name, addr)
	partition := &partition{
		name : name,
		addr : addr,
		broker: broker,
		isNativePartition:isNativePartiton,
		msgChan : make(chan *protocol.Message, msgChanSize),
		responseChan: make(chan *protocol.Server2Client),
		subscribedGroups: make(map[string] *subscribedGroup),
		//exitFinishedChan: make(chan string),
		msgAskChan: make(chan *msgAskData),
		exitChan: make(chan string),
	}
	partition.wg.Add(1)
	go func() {
		partition.readLoop()
		partition.wg.Done()
	}()
	//go partition.readLoop()
	return partition
}
func (p *partition) Put(msg *protocol.Message) *protocol.Server2Client{
	if !p.isNativePartition {
		myLogger.Logger.PrintWarning("try to put msg to not native partition")
		return nil
	}
	p.msgChan <- msg
	response := <- p.responseChan
	myLogger.Logger.Print("Put end")
	return response
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
	OuterLoop:
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
			if msg.Priority > 0{//这是优先级消息，需先判断消息是否可保存的优先队列
				for grpName, subGrp := range p.subscribedGroups{
					if subGrp.priorityQueue.Len() > queueSize - 2{//写不下了，需拒绝这条消息
						myLogger.Logger.Printf("group %s is full", grpName)
						response := &protocol.Server2Client{
							Key: protocol.Server2ClientKey_PriorityQueueFull,
						}
						p.responseChan <- response
						goto OuterLoop
					}
				}
			}
			for grpName, subGrp := range p.subscribedGroups{ //可正常发送
				myLogger.Logger.Printf("send msg to subscribedGroups %s", grpName)
				subGrp.readChan <- internalMsg
			}

			response := &protocol.Server2Client{
				Key: protocol.Server2ClientKey_PublishSuccess,
			}
			p.responseChan <- response
			p.subscribedGroupsLock.RUnlock()
		}
	}
exit:
	myLogger.Logger.Printf("Partition %s exiting", p.name)
	//p.exit()
}

func (p *partition) exit(){
	if !p.isNativePartition{ //不是本地的分区
		return
	}
	p.exitChan <- "bye"
	p.wg.Wait() //等待正常关闭
	p.subscribedGroupsLock.Lock()
	for _, subscribedGroup:= range p.subscribedGroups{
		subscribedGroup.exit()
		//<- subscribedGroup.exitFinishedChan
	}
	myLogger.Logger.Printf("Partition %s exit finished", p.name)
	p.subscribedGroupsLock.Unlock()
	//p.exitFinishedChan <- "bb"
	close(p.msgChan)
	close(p.msgAskChan)
	close(p.exitChan)
}

func (p *partition) getGroupRebalanceId( groupName string) int32{
	myLogger.Logger.Printf("partition %s  getGroupRebalanceId : %s", p.name, groupName)
	p.subscribedGroupsLock.RLock()
	defer p.subscribedGroupsLock.RUnlock()
	subscribedGroup, ok:= p.subscribedGroups[groupName]

	if !ok {
		return 0
	}else{
		myLogger.Logger.Printf("getGroupRebalanceId : %s",  subscribedGroup.rebalanceId)
		return subscribedGroup.rebalanceId
	}

	//subscribedGroup.consumerClientLock.Lock()
	//subscribedGroup.consumerClient = client
	//subscribedGroup.consumerClientLock.Unlock()
}

func (p *partition) addComsummerGroup(groupName string) {
	myLogger.Logger.Printf("partition %s  addComsummerClient : %s", p.name, groupName)
	p.subscribedGroupsLock.Lock()
	subscribedGroup, ok:= p.subscribedGroups[groupName]

	if !ok {
		subscribedGroup = newPartionGroup(groupName, p.name, p.broker)
		p.subscribedGroups[groupName] = subscribedGroup
	}
	p.subscribedGroupsLock.Unlock()

	myLogger.Logger.Printf("addComsummerGroup : %s", groupName)

}

func (p *partition) addComsummerGroupByInstance(group *subscribedGroup) {
	myLogger.Logger.Printf("partition %s  addComsummerClient : %s", p.name, group.name)
	p.subscribedGroupsLock.Lock()
	_, ok:= p.subscribedGroups[group.name]
	if !ok {
		p.subscribedGroups[group.name] = group
	}
	p.subscribedGroupsLock.Unlock()

	myLogger.Logger.Printf("addComsummerGroup : %s", group.name)

}

func (p *partition) addComsummerClient(client *client, groupName string, rebalanceId int32) {
	myLogger.Logger.Printf("partition %s  addComsummerClient : %s", p.name, groupName)
	p.subscribedGroupsLock.Lock()
	subscribedGroup, ok:= p.subscribedGroups[groupName]

	if !ok {
		subscribedGroup = newPartionGroup(groupName, p.name, p.broker)
		p.subscribedGroups[groupName] = subscribedGroup
	}
	subscribedGroup.rebalanceId = rebalanceId
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


