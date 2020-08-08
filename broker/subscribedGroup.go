package broker

import (
	"../mylib/myLogger"
	"../mylib/protocalFuc"
	"../protocol"
	"container/heap"
	"container/list"
	"github.com/golang/protobuf/proto"
	"sync"
	"time"
)


const retryTime = 1000 * time.Millisecond//队列重试时间
const MaxTryTime = 10 //最多重试的时间
const needReSend = false //是否需要重传
const showQueueSize = true //是否需要重传
type subscribedGroup struct {
	name string
	partitionName string
	rebalanceId int32
	wg sync.WaitGroup
	//consumerClientLock sync.RWMutex
	consumerClient *client //限制在IOloop进程中修改，不加锁
	circleQueue *circleQueue
	//circleQueue2 *circleQueue //高优先级队列
	priorityQueue *priorityQueue //高优先级队列
	//inflightQueue *list.List //链表

	inFlightMsgMap map[int32] *list.Element


	readChan chan *protocol.InternalMessage
	preparedMsgChan chan *protocol.InternalMessage
	clientChangeChan     chan *clientChange
	readLoopExitChan chan string
	writeLoopExitChan chan string
	//exitFinishedChan chan string
	msgAskChan     chan int32
	diskQueue1 *diskQueue
	queueSize int
	//diskQueue2 *diskQueue
}


func newPartionGroup(name string, partitionName string, broker *Broker) *subscribedGroup  {
	myLogger.Logger.Printf("newPartionGroup %s %s ", name, partitionName)
	g := &subscribedGroup{
		readChan: make(chan *protocol.InternalMessage),
		preparedMsgChan: make(chan *protocol.InternalMessage),
		clientChangeChan: make(chan *clientChange),
		msgAskChan: make(chan int32),
		readLoopExitChan: make(chan string),
		writeLoopExitChan: make(chan string),
		inFlightMsgMap: make(map[int32] *list.Element),
		circleQueue: NewCircleQueue( broker.queueSize+ 1),
		priorityQueue: NewPriorityQueue(broker.queueSize + 1),
		//inflightQueue: list.New(),
		partitionName: partitionName,
		name: name,
		diskQueue1: NewDiskQueue("./" + broker.Id + "data/" + partitionName + "/" + name + "/"),
		//diskQueue2: NewDiskQueue("./data/" + partitionName + "/" + name + "-p2/"),
		queueSize : broker.queueSize,
	}
	g.wg.Add(2)
	go func() {
		g.readLoop()
		g.wg.Done()
	}()
	go func() {
		g.writeLoop()
		g.wg.Done()
	}()
	return g
}


func (g *subscribedGroup) exit(){
	myLogger.Logger.Print("subscribedGroup exiting")
	g.writeLoopExitChan <- "bye"
	g.readLoopExitChan <- "bye"
	g.wg.Wait() //等待两个loop退出
	//for {//把inflight数据全部存入磁盘
	//	e := g.inflightQueue.Front()
	//	if e == nil{//读完了
	//		break
	//	}
	//	inflightMes := e.Value.(*protocol.InternalMessage)
	//	g.popInflightMsg(inflightMes.Id)
	//
	//	data, err := proto.Marshal(inflightMes)
	//	if err != nil{
	//		myLogger.Logger.PrintError("Marshal :", err)
	//	}
	//	err = g.diskQueue1.storeData(data)
	//	if err != nil{
	//		myLogger.Logger.PrintError("storeData :", err)
	//	}
	//	myLogger.Logger.Printf("push Msg %s to diskQueue, present Len: %d", inflightMes, g.diskQueue1.msgNum)
	//}
	for g.circleQueue.Len() > 0{ //把优先级队列1的数据全部存入磁盘
		myLogger.Logger.Print("read Data from priorityQueue")
		circleQueue2Data, _ := g.circleQueue.Pop()
		data, err := proto.Marshal(circleQueue2Data)
		if err != nil{
			myLogger.Logger.PrintError("Marshal :", err)
		}
		err = g.diskQueue1.storeData(data)
		if err != nil{
			myLogger.Logger.PrintError("storeData :", err)
		}
		myLogger.Logger.Printf("push Msg %s to diskQueue, present Len: %d", circleQueue2Data, g.diskQueue1.msgNum)
	}

	for g.priorityQueue.Len() > 0{ //把优先级队列的数据全部存入磁盘
		myLogger.Logger.Print("read Data from priorityQueue")
		priorityQueueData := heap.Pop(g.priorityQueue).(*protocol.InternalMessage)
		data, err := proto.Marshal(priorityQueueData)

		if err != nil{
			myLogger.Logger.PrintError("Marshal :", err)
		}
		err = g.diskQueue1.storeData(data)
		if err != nil{
			myLogger.Logger.PrintError("storeData :", err)
		}
		myLogger.Logger.Printf("push Msg %s to diskQueue, present Len: %d", priorityQueueData, g.diskQueue1.msgNum)
	}



	err := g.diskQueue1.sync()
	if err != nil{
		myLogger.Logger.PrintError("persistDiskData1 :", err)
	}
	myLogger.Logger.Print("subscribedGroup exit finished")
	//g.exitFinishedChan <- "bye "
	//myLogger.Logger.Print("subscribedGroup exit finished2")
	close(g.readChan)
	close(g.preparedMsgChan)
	close(g.clientChangeChan)
	close(g.msgAskChan)
	close(g.readLoopExitChan)
	close(g.writeLoopExitChan)

}

//func (g *subscribedGroup) pushInflightMsg(msg *protocol.InternalMessage) error{
//	msg.Timeout = time.Now().Add(retryTime).UnixNano() //设置超时时间
//	_, ok := g.inFlightMsgMap[msg.Id]
//	if ok {
//		return errors.New("pushInflightMsg already exist")
//	}
//	e := g.inflightQueue.PushBack(msg) //插入队列后面
//	myLogger.Logger.Printf("pushInflightMsg: %d  remain len: %d", msg.Id, g.inflightQueue.Len())
//
//	g.inFlightMsgMap[msg.Id] = e //保存插入位置
//	//heap.Push(g.inflightQueue, msg)
//	return nil
//}
//
//func (g *subscribedGroup) popInflightMsg(msgId int32) error{
//	e, ok := g.inFlightMsgMap[msgId]
//	if !ok {
//		return errors.New("popInflightMsg not exist")
//	}
//	g.inflightQueue.Remove(e)
//	myLogger.Logger.Printf("popInflightMsg:  %d  remain len: %d:", msgId, g.inflightQueue.Len())
//	//heap.Remove(g.inflightQueue, int(msg.Pos))
//	delete(g.inFlightMsgMap, msgId)
//	return nil
//}

func (g *subscribedGroup) writeLoop()  {
	var msg *protocol.InternalMessage
	var preparedMsgChan chan *protocol.InternalMessage
	var consumerChan chan []byte
	var curConsumerChan chan []byte
	var pushMsg []byte
	//retryTicker := time.NewTicker(retryTime) //定时检查inflightQ是否超时
	//retryTickerChan := retryTicker.C
	for{
		select {
		case <- g.writeLoopExitChan: //要退出了，保存好pushMsg中的临时数据，防止丢失
			goto exit
		case tmp := <- g.clientChangeChan:
			if tmp.isAdd{//添加或修改client
				g.consumerClient = tmp.client
				consumerChan = g.consumerClient.getWriteMsgChan() //可以往这边消费者写数据了
				if pushMsg == nil{//还没有消息，开始读
					preparedMsgChan = g.preparedMsgChan
					curConsumerChan = nil
				}else{//有消息，开始写
					preparedMsgChan = nil
					curConsumerChan = consumerChan
				}
				myLogger.Logger.Print("add group Client success")
			}else{//删除client
				if g.consumerClient != nil{ // && g.consumerClient.id == tmp.client.id id不等的话是因为已被替换了,不用处理
					g.consumerClient = nil
					preparedMsgChan = nil
					curConsumerChan = nil //没有消费者了，让其阻塞
					consumerChan = nil
					myLogger.Logger.Print("invalidate group Client")
				}
			}
			tmp.waitFinished <- true
		case msg = <- preparedMsgChan:
			//g.consumerClientLock.RLock() //防止写已关闭的client
			if g.consumerClient == nil{ //目前没有消费者连接
				myLogger.Logger.Printf("subscribedGroups %s invalid", g.name)
				//g.consumerClientLock.RUnlock()
				continue
			}
			myLogger.Logger.Printf("subscribedGroup %s readMsg %s", g.name, string(msg.Msg))


			tmp := &protocol.PushMsgReq{
				PartitionName: g.partitionName,
				GroupName: g.name,
				Msg: &protocol.Message{
					Id: msg.Id,
					Priority: msg.Priority,
					Msg: msg.Msg,
				},
			}


			data, err := proto.Marshal(tmp)
			if err != nil {
				myLogger.Logger.PrintError("marshaling error", err)
				return
			}
			pushMsg, err = protocalFuc.PackClientServerProtoBuf(protocol.ClientServerCmd_CmdPushMsgReq, data)
			if err != nil {
				myLogger.Logger.PrintError("PackClientServerProtoBuf error", err)
				return
			}

			preparedMsgChan = nil //读到数据，阻塞这里，等待pushMsg通过 consumerChan发送到消费者
			curConsumerChan = consumerChan

		case curConsumerChan  <- pushMsg:
			if consumerChan == nil {
				myLogger.Logger.PrintError("should not come here")
			}
			pushMsg = nil
			curConsumerChan = nil//发送完毕，阻塞这里，继续读数据
			preparedMsgChan = g.preparedMsgChan
		}
	}
exit:
	myLogger.Logger.Print("writeLoop exit")

}

func (g *subscribedGroup) readLoop()  {
	var msg *protocol.InternalMessage
	var memPreparedMsgChan chan *protocol.InternalMessage
	//var diskPreparedMsgChan chan *protocol.InternalMessage
	var diskReadyChan chan []byte
	var memReadyData *protocol.InternalMessage
	var diskReadyData *protocol.InternalMessage
	//var diskReadyData2 *protocol.InternalMessage
	//var haveProcessPreparedData bool = true
	var showQueueChan <- chan time.Time
	var showQueueTicker *time.Ticker

	var retryTickerChan <- chan time.Time
	var retryTicker *time.Ticker
	if needReSend { //需要超时重传时开启
		retryTicker = time.NewTicker(retryTime) //定时检查inflightQ是否超时
		retryTickerChan = retryTicker.C
	}
	if showQueueSize{ //定时显示磁盘及内存队列大小
		showQueueTicker = time.NewTicker(time.Second)
		showQueueChan = showQueueTicker.C
	}
	for{

		//负责读disk1中的消息到circleQueue1
		if diskReadyData == nil{ //准备好的数据为空
			diskReadyChan = g.diskQueue1.readyChan //可从磁盘读数据
		}else{//磁盘数据准备好了
			if g.circleQueue.Len() < g.queueSize{//放到内存队列
				g.circleQueue.Push(diskReadyData) //放到队尾
				myLogger.Logger.Printf("push Msg %s to circleQueue, present queueLen: %d present dataLen:", diskReadyData, g.circleQueue.Len(),  g.circleQueue.SentDataLen())
				diskReadyData = nil//清空
				diskReadyChan = g.diskQueue1.readyChan //可从磁盘读新数据
			}else{
				diskReadyChan = nil //读出的数据未处理，不可从磁盘读数据,使其阻塞
			}
		}

		//读取内存中的消息，准备发送
		if memReadyData == nil && g.priorityQueue.Len() > 0{//优先队列中有数据必须先发完优先队列中的
			myLogger.Logger.Print("read Data from priorityQueue")
			memReadyData = heap.Pop(g.priorityQueue).(*protocol.InternalMessage)
			//memReadyData, _ = g.circleQueue2.Pop()
		}else if memReadyData == nil && g.circleQueue.SentDataLen() > 0{ //只有优先队列中没有数据，才往这里读数据
			myLogger.Logger.Print("read Data from circleQueue1 remain Len：", g.circleQueue.SentDataLen())
			memReadyData, _ = g.circleQueue.PopSendData()
		}
		//else if memReadyData == nil && g.circleQueue.Len() > 0{ //只有优先队列中没有数据，才往这里读数据
		//	myLogger.Logger.Print("read Data from circleQueue1")
		//	memReadyData, _ = g.circleQueue.Pop()
		//}
		if memReadyData == nil{
			memPreparedMsgChan = nil //内存无数据可发，设为nil使其阻塞
		}else{
			memPreparedMsgChan = g.preparedMsgChan //内存有数据，管道准备好时应该发送
		}

		select {
		case askId := <-g.msgAskChan:
			myLogger.Logger.Print("receive delete ask: ", askId)
			//err := g.popInflightMsg(askId)
			for{
				msg, err := g.circleQueue.Peek()
				if err != nil{
					myLogger.Logger.PrintError(err)
					break
				}
				if askId > msg.Id{
					g.circleQueue.Pop() //该数据已被消费了，出队
				}else if askId == msg.Id{
					g.circleQueue.Pop() //该数据已被消费了，出队
					break
				}else{
					break
				}
			}
			//g.circleQueue.Pop()
		case <- g.readLoopExitChan: //要退出了，保存好这个磁盘中读出的数据，防止丢失
			if diskReadyData != nil{
				g.circleQueue.Push(diskReadyData)
			}
			//if memReadyData != nil{ //
			//	g.circleQueue.Push(memReadyData) //保证队列满存储也不出问题
			//}
			goto exit
		case bytes := <- diskReadyChan: //1有磁盘数据可读
			if diskReadyData != nil {
				myLogger.Logger.PrintError("Should not come here")
			}
			diskReadyData = &protocol.InternalMessage{}
			err := proto.Unmarshal(bytes, diskReadyData)
			if err != nil{
				myLogger.Logger.PrintError("Unmarshal :", err)
			}

		case memPreparedMsgChan <- memReadyData:
			if memReadyData == nil {
				myLogger.Logger.PrintError("Should not come here")
			}
			memReadyData = nil
		case msg = <-g.readChan://新数据到了
			msg.TryTimes++ //重试次数加一
			if msg.Priority == 0{ //普通消息
				if g.circleQueue.Len() < g.queueSize{// && g.diskQueue1.msgNum == 0，只有磁盘中的数据已经处理完了并且内存数据为0才可以存内存，保证顺序性
					if needReSend {
						msg.Timeout = time.Now().Add(retryTime).UnixNano() //设置超时时间
					}
					g.circleQueue.Push(msg)
					//heap.Push(g.priorityQueue, msg)
					myLogger.Logger.Printf("push Msg %s to circleQueue1, present Len: %d", msg, g.circleQueue.Len())
				}else{//存磁盘
					data, err := proto.Marshal(msg)
					if err != nil{
						myLogger.Logger.PrintError("Marshal :", err)
					}
					err = g.diskQueue1.storeData(data)
					if err != nil{
						myLogger.Logger.PrintError("storeData :", err)
					}
					myLogger.Logger.Printf("push Msg %s to diskQueue, present Len: %d", msg, g.diskQueue1.msgNum)
				}
			}else{//高优先级
				if g.priorityQueue.Len() < g.queueSize { //存优先级队列
					heap.Push(g.priorityQueue, msg)
					myLogger.Logger.Printf("push Msg %s to priorityQueue, present Len: %d", msg, g.priorityQueue.Len())
				}else{
					myLogger.Logger.PrintError("should not come here")
				}
			}
		case <- retryTickerChan: //时间到，检测是否需要重传消息
			msg, err := g.circleQueue.Peek()
			if err != nil{
				myLogger.Logger.PrintError(err)
			}
			if msg.Timeout < time.Now().UnixNano() { //超时了
				g.circleQueue.ResetSendData() //将发送位移重置为队首元素
				break
			}
		case <- showQueueChan: //时间到，检测是否需要重传消息
			//myLogger.Logger.PrintfDebug("partitionName: %s circleQueue SentDataLen: %d  circleQueue Len: %d  diskQueue size: %d ",
			//	g.partitionName,g.circleQueue.SentDataLen(), g.circleQueue.Len(), g.diskQueue1.msgNum)
			myLogger.Logger.PrintfDebug("分区名: %s  内存队列长度: %d",
				g.partitionName, g.circleQueue.Len())
		}

	}
	exit:
		myLogger.Logger.Print("readLoop exit")
		if needReSend{
			retryTicker.Stop()
		}
		if showQueueSize{
			showQueueTicker.Stop()
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