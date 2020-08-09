package broker

import (
	"../mylib/myLogger"
	"../mylib/protocalFuc"
	"../protocol"
	"github.com/golang/protobuf/proto"
	"math"
	"sync"
)

type partition struct {
	sync.RWMutex
	name                 string
	addr                 string
	belongtopic          string
	subscribedGroupsLock sync.RWMutex
	subscribedGroups     map[string]*subscribedGroup //groupName -> *subscribedGroup
	curMsgId             int32
	wg                   sync.WaitGroup

	broker       *Broker
	msgChan      chan *protocol.Message
	responseChan chan []byte //*protocol.Server2Client
	msgAskChan   chan *msgAskData
	//exitFinishedChan chan string
	exitChan chan string

	isNativePartition bool //判断该分区是否在本地broker中
	queueSize         int
}

const (
	msgChanSize = 0
)

type msgAskData struct {
	msgId     int32
	groupName string
}

func newPartition(name string, addr string, isNativePartiton bool, broker *Broker, belongtopic string) *partition {
	if !isNativePartiton { //不是本地的分区
		partition := &partition{
			name:              name,
			addr:              addr,
			broker:            broker,
			queueSize:         broker.queueSize,
			isNativePartition: isNativePartiton,
			belongtopic:       belongtopic,
		}
		return partition
	}

	myLogger.Logger.Printf("newPartition %s : %s", name, addr)
	partition := &partition{
		name:              name,
		addr:              addr,
		broker:            broker,
		isNativePartition: isNativePartiton,
		msgChan:           make(chan *protocol.Message, msgChanSize),
		responseChan:      make(chan []byte),
		subscribedGroups:  make(map[string]*subscribedGroup),
		//exitFinishedChan: make(chan string),
		msgAskChan: make(chan *msgAskData),
		exitChan:   make(chan string),
	}
	partition.wg.Add(1)
	go func() {
		partition.readLoop()
		partition.wg.Done()
	}()
	//go partition.readLoop()
	return partition
}

//往分区中放数据
func (p *partition) Put(msg *protocol.Message) []byte {
	if !p.isNativePartition {
		myLogger.Logger.PrintWarning("try to put msg to not native partition")
		return nil
	}
	p.msgChan <- msg
	response := <-p.responseChan
	myLogger.Logger.Print("Put end")
	return response
}

//生产id
func (p *partition) generateMsgId() int32 {
	var nextId int32
	if p.curMsgId == math.MaxInt32 {
		//atomic.StoreInt32(&p.curMsgId, 0)
		p.curMsgId = 0
		nextId = 0
	} else {
		//nextId = atomic.AddInt32(&p.curMsgId, 1)
		p.curMsgId++
		nextId = p.curMsgId
	}
	return nextId
}

//读取发布到分区的数据，ask或发布消息
func (p *partition) readLoop() {
	var msg *protocol.Message
	for {
	OuterLoop:
		select {
		case <-p.exitChan:
			goto exit
		case msgAskData := <-p.msgAskChan:
			p.subscribedGroupsLock.RLock()
			subscribedGroup, ok := p.subscribedGroups[msgAskData.groupName]
			if !ok {
				myLogger.Logger.Print("msgAskData group not exist :", msgAskData.groupName)
			}
			subscribedGroup.msgAskChan <- msgAskData.msgId
			p.subscribedGroupsLock.RUnlock()
		case msg = <-p.msgChan:
			nextId := p.generateMsgId()
			internalMsg := &protocol.InternalMessage{
				Id:       nextId,
				Priority: msg.Priority,
				Msg:      msg.Msg,
			}
			myLogger.Logger.Printf("Partition %s readMsg %s", p.name, string(msg.Msg))
			p.subscribedGroupsLock.RLock()
			if msg.Priority > 0 { //这是优先级消息，需先判断消息是否可保存的优先队列
				for grpName, subGrp := range p.subscribedGroups {
					if subGrp.priorityQueue.Len() > p.queueSize-2 { //写不下了，需拒绝这条消息
						myLogger.Logger.Printf("group %s is full", grpName)
						rsp := &protocol.PushMsgRsp{
							Ret: protocol.RetStatus_QueueFull,
						}
						data, err := proto.Marshal(rsp)
						if err != nil {
							myLogger.Logger.PrintError("marshaling error", err)
							continue
						}
						reqData, err := protocalFuc.PackClientServerProtoBuf(protocol.ClientServerCmd_CmdPushMsgRsp, data)
						if err != nil {
							myLogger.Logger.PrintError("marshaling error", err)
							continue
						}
						myLogger.Logger.Printf("write: %s", rsp)
						p.responseChan <- reqData
						p.subscribedGroupsLock.RUnlock()
						goto OuterLoop
					}
				}
			}
			for grpName, subGrp := range p.subscribedGroups { //可正常发送，进行消息分发
				myLogger.Logger.Printf("send msg to subscribedGroups %s", grpName)
				subGrp.readChan <- internalMsg
			}
			p.subscribedGroupsLock.RUnlock()

			rsp := &protocol.PushMsgRsp{
				Ret: protocol.RetStatus_Successs,
			}
			data, err := proto.Marshal(rsp)
			if err != nil {
				myLogger.Logger.PrintError("marshaling error", err)
				continue
			}
			reqData, err := protocalFuc.PackClientServerProtoBuf(protocol.ClientServerCmd_CmdPushMsgRsp, data)
			if err != nil {
				myLogger.Logger.PrintError("marshaling error", err)
				continue
			}
			myLogger.Logger.Printf("write: %s %s", protocol.ClientServerCmd_CmdPushMsgRsp, rsp)

			p.responseChan <- reqData
		}
	}
exit:
	myLogger.Logger.Printf("Partition %s exiting", p.name)
	//p.exit()
}

func (p *partition) exit() {
	if !p.isNativePartition { //不是本地的分区
		return
	}
	p.exitChan <- "bye"
	p.wg.Wait() //等待正常关闭
	p.subscribedGroupsLock.Lock()
	for _, subscribedGroup := range p.subscribedGroups {
		subscribedGroup.exit()
	}
	myLogger.Logger.Printf("Partition %s exit finished", p.name)
	p.subscribedGroupsLock.Unlock()
	close(p.msgChan)
	close(p.msgAskChan)
	close(p.exitChan)
}

func (p *partition) getGroupRebalanceId(groupName string) int32 {
	myLogger.Logger.Printf("partition %s  getGroupRebalanceId : %s", p.name, groupName)
	p.subscribedGroupsLock.RLock()
	defer p.subscribedGroupsLock.RUnlock()
	subscribedGroup, ok := p.subscribedGroups[groupName]

	if !ok {
		return 0
	} else {
		myLogger.Logger.Printf("getGroupRebalanceId : %s", subscribedGroup.rebalanceId)
		return subscribedGroup.rebalanceId
	}
}

func (p *partition) addComsummerGroup(groupName string) {
	myLogger.Logger.Printf("partition %s  addComsummerClient : %s", p.name, groupName)
	p.subscribedGroupsLock.Lock()
	subscribedGroup, ok := p.subscribedGroups[groupName]

	if !ok {
		subscribedGroup = NewSubscribedGroup(groupName, p.name, p.broker)
		p.subscribedGroups[groupName] = subscribedGroup
	}
	p.subscribedGroupsLock.Unlock()

	myLogger.Logger.Printf("addComsummerGroup : %s", groupName)

}

func (p *partition) addComsummerGroupByInstance(group *subscribedGroup) {
	myLogger.Logger.Printf("partition %s  addComsummerClient : %s", p.name, group.name)
	p.subscribedGroupsLock.Lock()
	_, ok := p.subscribedGroups[group.name]
	if !ok {
		p.subscribedGroups[group.name] = group
	}
	p.subscribedGroupsLock.Unlock()

	myLogger.Logger.Printf("addComsummerGroup : %s", group.name)

}

func (p *partition) addComsummerClient(client *client, groupName string, rebalanceId int32) {
	myLogger.Logger.Printf("partition %s  addComsummerClient : %s", p.name, groupName)
	p.subscribedGroupsLock.Lock()
	subscribedGroup, ok := p.subscribedGroups[groupName]

	if !ok {
		subscribedGroup = NewSubscribedGroup(groupName, p.name, p.broker)
		p.subscribedGroups[groupName] = subscribedGroup
	}
	subscribedGroup.rebalanceId = rebalanceId
	waitFinished := make(chan bool)

	subscribedGroup.clientChangeChan <- &clientChange{true, client, waitFinished} //放到partitiongroup的readLoop协程中进行处理
	<-waitFinished                                                                //等待处理
	p.subscribedGroupsLock.Unlock()

	myLogger.Logger.Printf("addComsummerClient : %s", groupName)

}

func (p *partition) invalidComsummerClient(client *client, groupName string) { //使得client失效，因为已经被删除
	p.subscribedGroupsLock.Lock()
	g, ok := p.subscribedGroups[groupName]
	if !ok {
		myLogger.Logger.Print("invalidComsummerClient not exist : ", groupName)
	} else {
		waitFinished := make(chan bool)
		g.clientChangeChan <- &clientChange{false, client, waitFinished} //放到partitiongroup的readLoop协程中进行处理，避免频繁使用锁
		<-waitFinished                                                   //等待处理
		myLogger.Logger.Printf("invalidComsummerClient : %s", groupName)
	}
	p.subscribedGroupsLock.Unlock()
}

func (p *partition) deleteComsummerClient(client *client, groupName string) {
	p.subscribedGroupsLock.Lock()
	g, ok := p.subscribedGroups[groupName]
	if !ok {
		myLogger.Logger.Print("deleteComsummerClient not exist : ", groupName)
	} else {

		waitFinished := make(chan bool)
		g.clientChangeChan <- &clientChange{false, client, waitFinished} //放到partitiongroup的readLoop协程中进行处理，避免频繁使用锁
		<-waitFinished                                                   //等待处理
		myLogger.Logger.Printf("invalidComsummerClient : %s", groupName)

	}
	p.subscribedGroupsLock.Unlock()
	return
}
