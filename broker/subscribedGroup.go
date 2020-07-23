package broker

import (
	"../mylib/myLogger"
	"../protocol"
	"container/heap"
	"container/list"
	"errors"
	"time"
)

const queueSize = 1024*1024 //队列大小
const retryTime = 1000 * time.Millisecond //队列重试时间
type subscribedGroup struct {
	name string
	partitionName string
	readChan chan *protocol.InternalMessage
	preparedMsgChan chan *protocol.InternalMessage
	//consumerClientLock sync.RWMutex
	consumerClient *client //限制在IOloop进程中修改，不加锁
	priorityQueue *priorityQueue
	inflightQueue *list.List //链表

	inFlightMsgMap map[int32] *list.Element

	clientChangeChan     chan *clientChange
	msgAskChan     chan int32
}


func newPartionGroup(name, partitionName string) *subscribedGroup  {
	g := &subscribedGroup{
		readChan: make(chan *protocol.InternalMessage),
		preparedMsgChan: make(chan *protocol.InternalMessage),
		clientChangeChan: make(chan *clientChange),
		msgAskChan: make(chan int32),
		inFlightMsgMap: make(map[int32] *list.Element),
		priorityQueue: NewPriorityQueue(queueSize),
		inflightQueue: list.New(),
		partitionName: partitionName,
		name: name,
	}
	go g.readLoop()
	go g.writeLoop()
	return g
}
func (g *subscribedGroup) pushInflightMsg(msg *protocol.InternalMessage) error{
	msg.Timeout = time.Now().Add(retryTime).UnixNano() //设置超时时间
	_, ok := g.inFlightMsgMap[msg.Id]
	if ok {
		return errors.New("pushInflightMsg already exist")
	}
	e := g.inflightQueue.PushBack(msg) //插入队列后面
	myLogger.Logger.Printf("pushInflightMsg: %d  remain len: %d", msg.Id, g.inflightQueue.Len())

	g.inFlightMsgMap[msg.Id] = e //保存插入位置
	//heap.Push(g.inflightQueue, msg)
	return nil
}

func (g *subscribedGroup) popInflightMsg(msgId int32) error{
	e, ok := g.inFlightMsgMap[msgId]
	if !ok {
		return errors.New("popInflightMsg not exist")
	}
	g.inflightQueue.Remove(e)
	myLogger.Logger.Printf("popInflightMsg:  %d  remain len: %d:", msgId, g.inflightQueue.Len())
	//heap.Remove(g.inflightQueue, int(msg.Pos))
	delete(g.inFlightMsgMap, msgId)
	return nil
}
func (g *subscribedGroup) writeLoop()  {
	var msg *protocol.InternalMessage
	var preparedMsgChan chan *protocol.InternalMessage
	var consumerChan chan *protocol.Server2Client
	var curConsumerChan chan *protocol.Server2Client
	var pushMsg *protocol.Server2Client
	retryTicker := time.NewTicker(retryTime) //定时检查inflightQ是否超时
	retryTickerChan := retryTicker.C
	//retryTickerChan = nil
	for{
		select {
		case tmp := <- g.clientChangeChan:
			if tmp.isAdd{//添加或修改client
				g.consumerClient = tmp.client
				consumerChan = g.consumerClient.writeChan //可以往这边消费者写数据了
				if pushMsg == nil{//还没有消息，开始读
					preparedMsgChan = g.preparedMsgChan
					curConsumerChan = nil
				}else{//有消息，开始写
					preparedMsgChan = nil
					curConsumerChan = consumerChan
				}
				myLogger.Logger.Print("add group Client success")
			}else{//删除client
				if g.consumerClient != nil && g.consumerClient.id == tmp.client.id{ //id不等的话是因为已被替换了,不用处理
					g.consumerClient = nil
					preparedMsgChan = nil
					curConsumerChan = nil //没有消费者了，让其阻塞
					consumerChan = nil
					myLogger.Logger.Print("invalidate group Client")
				}
			}
			tmp.waitFinished <- true
		case askId := <-g.msgAskChan:
			myLogger.Logger.Print("receive delete ask: ", askId)
			err := g.popInflightMsg(askId)
			if err != nil{
				myLogger.Logger.PrintError(err)
			}
		case msg = <- preparedMsgChan:
			//g.consumerClientLock.RLock() //防止写已关闭的client
			if g.consumerClient == nil{ //目前没有消费者连接
				myLogger.Logger.Printf("subscribedGroups %s invalid", g.name)
				//g.consumerClientLock.RUnlock()
				continue
			}
			myLogger.Logger.Printf("subscribedGroup %s readMsg %s", g.name, string(msg.Msg))
			pushMsg = &protocol.Server2Client{ //封装
				Key: protocol.Server2ClientKey_PushMsg,
				MsgGroupName: g.name,
				MsgPartitionName: g.partitionName,
				Msg: &protocol.Message{
					Id: msg.Id,
					Priority: msg.Priority,
					Msg: msg.Msg,
				},
			}
			preparedMsgChan = nil //读到数据，阻塞这里，等待pushMsg通过 consumerChan发送到消费者
			curConsumerChan = consumerChan
			//g.consumerClient.writeChan <- pushMsg
			//g.consumerClientLock.RUnlock()
		case curConsumerChan  <- pushMsg:
			err := g.pushInflightMsg(msg)
			if err != nil{
				myLogger.Logger.PrintError(err)
			}
			if consumerChan == nil {
				myLogger.Logger.PrintError("should not come here")
			}
			pushMsg = nil
			curConsumerChan = nil//发送完毕，阻塞这里，继续读数据
			preparedMsgChan = g.preparedMsgChan

		case <- retryTickerChan: //时间到，检查inflight队列是否超时,循环把超时的消息都重传
			for {
				e := g.inflightQueue.Front()
				if e == nil{
					break
				}
				inflightMes := e.Value.(*protocol.InternalMessage)
				if inflightMes.Timeout < time.Now().UnixNano() { //未超时，不处理
					break
				}
				myLogger.Logger.Print("intflightQueue mes time out")
				g.readChan <- inflightMes //此管道阻塞时，会有存磁盘操作，所以不会长时间阻塞，重新给group消费
				g.popInflightMsg(inflightMes.Id)
			}

		}


	}
}

func (g *subscribedGroup) readLoop()  {
	var msg *protocol.InternalMessage
	var preparedMsgChan chan *protocol.InternalMessage
	var readyData *protocol.InternalMessage
	//var haveProcessPreparedData bool = true
	for{
		if readyData == nil && g.priorityQueue.Len() > 0{
			myLogger.Logger.Print("read Data from priorityQueue")
			readyData = heap.Pop(g.priorityQueue).(*protocol.InternalMessage)
		}
		if readyData == nil{
			preparedMsgChan = nil //无数据可发，设为nil使其阻塞
		}else{
			preparedMsgChan = g.preparedMsgChan //有数据，管道准备好时应该发送
		}
		select {
		case preparedMsgChan <- readyData:
			if readyData == nil {
				myLogger.Logger.Print("error, should not come here")
			}
			readyData = nil
		case msg = <-g.readChan:
			if g.priorityQueue.Len() < queueSize{//存内存
				msg.TryTimes++ //重试次数加一
				heap.Push(g.priorityQueue, msg)
				myLogger.Logger.Printf("push Msg %s to priorityQueue, present Len: %d", msg, g.priorityQueue.Len())
			}else{//存磁盘

			}
		}

	}
}

//func (g *subscribedGroup) readLoop()  {
//	var msg *protocol.Message
//	for{
//		select {
//		case msg = <-g.msgChan:
//		}
//		g.consumerClientLock.RLock() //防止写已关闭的client
//		if g.consumerClient == nil{ //目前没有消费者连接
//			myLogger.Logger.Printf("subscribedGroups %s invalid", g.name)
//			g.consumerClientLock.RUnlock()
//			continue
//		}
//
//		myLogger.Logger.Printf("subscribedGroup %s readMsg %s", g.name, string(msg.Msg))
//		response := &protocol.Server2Client{
//			Key: protocol.Server2ClientKey_PushMsg,
//			Msg: msg,
//		}
//		g.consumerClient.writeChan <- response
//		g.consumerClientLock.RUnlock()
//	}
//}