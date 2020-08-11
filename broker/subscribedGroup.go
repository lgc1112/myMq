package broker

import (
	"../mylib/myLogger"
	"../mylib/protocalFuc"
	"../protocol"
	"container/heap"
	"github.com/golang/protobuf/proto"
	"sync"
	"time"
)

const retryTime = 1000 * time.Millisecond //队列重试时间
const MaxTryTime = 10                     //最多重试的时间
const needReSend = false                  //是否需要重传
const showQueueSize = false               //是否需要重传
type subscribedGroup struct {
	name           string
	partitionName  string
	rebalanceId    int32
	wg             sync.WaitGroup
	consumerClient *client //限制在IOloop进程中修改，不加锁
	circleQueue    *circleQueue
	priorityQueue  *priorityQueue //高优先级队列
	inflightQueue  *circleQueue   //循环队列，保存优先级队列中未ack的数据

	readChan           chan *protocol.InternalMessage
	preparedMsgChan    chan *protocol.InternalMessage
	clientChangeChan   chan *clientChange
	readLoopExitChan   chan string
	writeLoopExitChan  chan string
	msgAskChan         chan int32
	priorityMsgAskChan chan int32
	diskQueue1         *diskQueue //堆积队列
	diskQueue2         *diskQueue //持久化及恢复队列
	queueSize          int
}

func NewSubscribedGroup(name string, partitionName string, broker *Broker) *subscribedGroup {
	myLogger.Logger.Printf("newPartionGroup %s %s ", name, partitionName)
	g := &subscribedGroup{
		readChan:           make(chan *protocol.InternalMessage),
		preparedMsgChan:    make(chan *protocol.InternalMessage),
		clientChangeChan:   make(chan *clientChange),
		msgAskChan:         make(chan int32),
		priorityMsgAskChan: make(chan int32),
		readLoopExitChan:   make(chan string),
		writeLoopExitChan:  make(chan string),
		circleQueue:        NewCircleQueue(broker.queueSize + 1),
		priorityQueue:      NewPriorityQueue(broker.queueSize + 1),
		inflightQueue:      NewCircleQueue(broker.queueSize + 1),
		partitionName:      partitionName,
		name:               name,
		diskQueue1:         NewDiskQueue("./" + broker.Id + "data/" + partitionName + "/" + name + "/"),
		diskQueue2:         NewDiskQueue("./" + broker.Id + "data/" + partitionName + "/" + name + "2/"),
		queueSize:          broker.queueSize,
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

//退出，回收资源，保存内存队列的数据到磁盘
func (g *subscribedGroup) exit() {
	myLogger.Logger.Print("subscribedGroup exiting")
	g.writeLoopExitChan <- "bye"
	g.readLoopExitChan <- "bye"
	g.wg.Wait() //等待两个loop退出

	g.diskQueue2.StopRead() //该停止读数据了

	for g.inflightQueue.Len() > 0 { //把inflightQueue队列的数据全部存入磁盘
		if g.priorityQueue.Len() < g.queueSize {
			myLogger.Logger.Print("read Data from inflightQueue")
			circleQueue2Data, _ := g.inflightQueue.Pop()
			g.priorityQueue.Push(circleQueue2Data)
		} else {
			myLogger.Logger.Print("read Data from inflightQueue")
			circleQueue2Data, _ := g.inflightQueue.Pop()
			data, err := proto.Marshal(circleQueue2Data)
			if err != nil {
				myLogger.Logger.PrintError("Marshal :", err)
			}
			err = g.diskQueue2.StoreData(data)
			if err != nil {
				myLogger.Logger.PrintError("storeData :", err)
			}
			myLogger.Logger.Printf("push Msg %s to diskQueue, present Len: %d", circleQueue2Data, g.diskQueue2.msgNum)
		}
	}

	for g.priorityQueue.Len() > 0 { //把优先级队列的数据全部存入磁盘
		myLogger.Logger.Print("read Data from priorityQueue")
		priorityQueueData := heap.Pop(g.priorityQueue).(*protocol.InternalMessage)
		data, err := proto.Marshal(priorityQueueData)

		if err != nil {
			myLogger.Logger.PrintError("Marshal :", err)
		}
		err = g.diskQueue2.StoreData(data)
		if err != nil {
			myLogger.Logger.PrintError("storeData :", err)
		}
		myLogger.Logger.Printf("push Msg %s to diskQueue, present Len: %d", priorityQueueData, g.diskQueue2.msgNum)
	}

	for g.circleQueue.Len() > 0 { //把循环队列的数据全部存入磁盘
		myLogger.Logger.Print("read Data from priorityQueue")
		circleQueue2Data, _ := g.circleQueue.Pop()
		data, err := proto.Marshal(circleQueue2Data)
		if err != nil {
			myLogger.Logger.PrintError("Marshal :", err)
		}
		err = g.diskQueue2.StoreData(data)
		if err != nil {
			myLogger.Logger.PrintError("storeData :", err)
		}
		myLogger.Logger.Printf("push Msg %s to diskQueue, present Len: %d", circleQueue2Data, g.diskQueue2.msgNum)
	}

	err := g.diskQueue2.sync() //刷盘
	if err != nil {
		myLogger.Logger.PrintError("persistDiskData1 :", err)
	}
	g.diskQueue2.Close()

	err = g.diskQueue1.sync() //刷盘
	if err != nil {
		myLogger.Logger.PrintError("persistDiskData1 :", err)
	}
	g.diskQueue1.Close()
	myLogger.Logger.Print("subscribedGroup exit finished")
	close(g.readChan)
	close(g.preparedMsgChan)
	close(g.clientChangeChan)
	close(g.msgAskChan)
	close(g.readLoopExitChan)
	close(g.writeLoopExitChan)

}

//写数据到client
func (g *subscribedGroup) writeLoop() {
	var msg *protocol.InternalMessage
	var preparedMsgChan chan *protocol.InternalMessage
	var consumerChan chan []byte
	var curConsumerChan chan []byte
	var pushMsg []byte
	//retryTicker := time.NewTicker(retryTime) //定时检查inflightQ是否超时
	//retryTickerChan := retryTicker.C
	for {
		select {
		case <-g.writeLoopExitChan: //要退出了，保存好pushMsg中的临时数据，防止丢失
			goto exit
		case tmp := <-g.clientChangeChan:
			if tmp.isAdd { //添加或修改client
				g.consumerClient = tmp.client
				consumerChan = g.consumerClient.getWriteMsgChan() //可以往这边消费者写数据了
				if pushMsg == nil {                               //还没有消息，开始读
					preparedMsgChan = g.preparedMsgChan
					curConsumerChan = nil
				} else { //有消息，开始写
					preparedMsgChan = nil
					curConsumerChan = consumerChan
				}
				//g.circleQueue.ResetSendData() //将发送位移重置为队首元素
				myLogger.Logger.Print("add group Client success")
			} else { //删除client
				if g.consumerClient != nil { // && g.consumerClient.id == tmp.client.id id不等的话是因为已被替换了,不用处理
					g.consumerClient = nil
					preparedMsgChan = nil
					curConsumerChan = nil //没有消费者了，让其阻塞
					consumerChan = nil
					myLogger.Logger.Print("invalidate group Client")
				}
			}
			tmp.waitFinished <- true
		case msg = <-preparedMsgChan:
			//g.consumerClientLock.RLock() //防止写已关闭的client
			if g.consumerClient == nil { //目前没有消费者连接
				myLogger.Logger.Printf("subscribedGroups %s invalid", g.name)
				//g.consumerClientLock.RUnlock()
				continue
			}
			myLogger.Logger.Printf("subscribedGroup %s readMsg %s", g.name, string(msg.Msg))

			tmp := &protocol.PushMsgReq{
				PartitionName: g.partitionName,
				GroupName:     g.name,
				Msg: &protocol.Message{
					Id:       msg.Id,
					Priority: msg.Priority,
					Msg:      msg.Msg,
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

		case curConsumerChan <- pushMsg:
			if consumerChan == nil {
				myLogger.Logger.PrintError("should not come here")
			}
			pushMsg = nil
			curConsumerChan = nil //发送完毕，阻塞这里，继续读数据
			preparedMsgChan = g.preparedMsgChan
		}
	}
exit:
	myLogger.Logger.Print("writeLoop exit")

}

//读取分区发送过来的数据
func (g *subscribedGroup) readLoop() {
	var msg *protocol.InternalMessage
	var memPreparedMsgChan chan *protocol.InternalMessage
	//var diskPreparedMsgChan chan *protocol.InternalMessage
	var diskReadyChan chan []byte
	var diskReadyChan2 chan []byte
	var memReadyData *protocol.InternalMessage
	var diskReadyData *protocol.InternalMessage
	var diskReadyData2 *protocol.InternalMessage
	//var haveProcessPreparedData bool = true
	var showQueueChan <-chan time.Time
	var showQueueTicker *time.Ticker

	var retryTickerChan <-chan time.Time
	var retryTicker *time.Ticker
	if needReSend { //需要超时重传时开启
		retryTicker = time.NewTicker(retryTime) //定时检查inflightQ是否超时
		retryTickerChan = retryTicker.C
	}
	if showQueueSize { //定时显示磁盘及内存队列大小
		showQueueTicker = time.NewTicker(time.Second)
		showQueueChan = showQueueTicker.C
	}
	for {

		//负责读disk1中的消息到circleQueue1
		if diskReadyData == nil { //准备好的数据为空
			diskReadyChan = g.diskQueue1.readyChan //可从磁盘读数据
		} else { //磁盘数据准备好了
			if g.circleQueue.Len() < g.queueSize { //放到内存队列
				g.circleQueue.Push(diskReadyData) //放到队尾
				myLogger.Logger.Printf("push Msg %s to circleQueue, present queueLen: %d present dataLen:", diskReadyData, g.circleQueue.Len(), g.circleQueue.SentDataLen())
				diskReadyData = nil                    //清空
				diskReadyChan = g.diskQueue1.readyChan //可从磁盘读新数据
			} else {
				diskReadyChan = nil //读出的数据未处理，不可从磁盘读数据,使其阻塞
			}
		}

		//负责读disk2中的消息到circleQueue或priorityQueue
		if diskReadyData2 == nil { //准备好的数据为空
			diskReadyChan2 = g.diskQueue2.readyChan //可从磁盘读数据
		} else { //磁盘数据准备好了
			if diskReadyData2.Priority == 0 { //优先级为0的数据
				if g.circleQueue.Len() < g.queueSize { //放到内存队列
					g.circleQueue.Push(diskReadyData2) //放到队列末尾
					myLogger.Logger.Printf("push Msg %s to circleQueue, present queueLen: %d present dataLen:", diskReadyData, g.circleQueue.Len(), g.circleQueue.SentDataLen())
					diskReadyData2 = nil                    //清空
					diskReadyChan2 = g.diskQueue2.readyChan //可从磁盘读新数据
				} else {
					diskReadyChan2 = nil //读出的数据未处理，不可从磁盘读数据,使其阻塞
				}
			} else {
				if g.priorityQueue.Len() < g.queueSize { //放到内存队列
					g.priorityQueue.Push(diskReadyData2) //放到队列末尾
					myLogger.Logger.Printf("push Msg %s to priorityQueue, present queueLen: %d:", diskReadyData, g.priorityQueue.Len())
					diskReadyData2 = nil                    //清空
					diskReadyChan2 = g.diskQueue2.readyChan //可从磁盘读新数据
				} else {
					diskReadyChan2 = nil //读出的数据未处理，不可从磁盘读数据,使其阻塞
				}
			}
		}

		//读取内存中的消息，准备发送
		if memReadyData == nil && g.priorityQueue.Len() > 0 { //优先队列中有数据必须先发完优先队列中的
			myLogger.Logger.Print("read Data from priorityQueue")
			memReadyData = heap.Pop(g.priorityQueue).(*protocol.InternalMessage)
			if needReSend {
				memReadyData.Timeout = time.Now().Add(retryTime).UnixNano() //设置超时时间
			}
			g.inflightQueue.Push(memReadyData) //保存到inflight队列
			//memReadyData, _ = g.circleQueue2.Pop()
		} else if memReadyData == nil && g.circleQueue.SentDataLen() > 0 { //只有优先队列中没有数据，才往这里读数据
			myLogger.Logger.Print("read Data from circleQueue1 remain Len：", g.circleQueue.SentDataLen())
			memReadyData, _ = g.circleQueue.PopSendData()
		}
		//else if memReadyData == nil && g.circleQueue.Len() > 0{ //只有优先队列中没有数据，才往这里读数据
		//	myLogger.Logger.Print("read Data from circleQueue1")
		//	memReadyData, _ = g.circleQueue.Pop()
		//}
		if memReadyData == nil {
			memPreparedMsgChan = nil //内存无数据可发，设为nil使其阻塞
		} else {
			memPreparedMsgChan = g.preparedMsgChan //内存有数据，管道准备好时应该发送
		}

		select {
		case askId := <-g.msgAskChan: //普通消息ack
			myLogger.Logger.Print("receive delete ask: ", askId)
			//err := g.popInflightMsg(askId)
			for {
				msg, err := g.circleQueue.Peek()
				if err != nil {
					myLogger.Logger.Print(err)
					break
				}
				if askId > msg.Id {
					g.circleQueue.Pop() //该数据已被消费了，出队
				} else if askId == msg.Id {
					g.circleQueue.Pop() //该数据已被消费了，出队
					break
				} else {
					myLogger.Logger.PrintWarning("delete ask can not find", askId)
					break
				}
			}
		case askId := <-g.priorityMsgAskChan: //优先级消息ack
			myLogger.Logger.Printf("receive priorityM delete ask: %d len %d", askId, g.inflightQueue.Len())
			//err := g.popInflightMsg(askId)
			for {
				msg, err := g.inflightQueue.Peek()
				if err != nil {
					myLogger.Logger.Print(err)
					break
				}
				if askId != msg.Id {
					g.inflightQueue.Pop() //该数据已被消费了，出队
				} else if askId == msg.Id {
					g.inflightQueue.Pop() //该数据已被消费了，出队
					break
				} else {
					myLogger.Logger.PrintWarning("delete ask can not find", askId)
					break
				}
			}
			//g.circleQueue.Pop()
		case <-g.readLoopExitChan: //要退出了，保存好这个磁盘中读出的数据，防止丢失
			if diskReadyData != nil {
				g.circleQueue.Push(diskReadyData)
			}
			goto exit
		case bytes := <-diskReadyChan: //1有磁盘数据可读
			if diskReadyData != nil {
				myLogger.Logger.PrintError("Should not come here")
			}
			diskReadyData = &protocol.InternalMessage{}
			err := proto.Unmarshal(bytes, diskReadyData)
			if err != nil {
				myLogger.Logger.PrintError("Unmarshal :", err)
			}

		case bytes := <-diskReadyChan2: //2有磁盘数据可读
			if diskReadyData2 != nil {
				myLogger.Logger.PrintError("Should not come here")
			}
			diskReadyData2 = &protocol.InternalMessage{}
			err := proto.Unmarshal(bytes, diskReadyData2)
			if err != nil {
				myLogger.Logger.PrintError("Unmarshal :", err)
			}

		case memPreparedMsgChan <- memReadyData:
			if memReadyData == nil {
				myLogger.Logger.PrintError("Should not come here")
			}
			memReadyData = nil
		case msg = <-g.readChan: //新数据到了
			msg.TryTimes++         //重试次数加一
			if msg.Priority == 0 { //普通消息
				if g.circleQueue.Len() < g.queueSize && g.diskQueue1.msgNum == 0 { // && g.diskQueue1.msgNum == 0，只有磁盘中的数据已经处理完了并且内存数据为0才可以存内存，保证顺序性
					if needReSend {
						msg.Timeout = time.Now().Add(retryTime).UnixNano() //设置超时时间
					}
					g.circleQueue.Push(msg)
					//heap.Push(g.priorityQueue, msg)
					myLogger.Logger.Printf("push Msg %s to circleQueue1, present Len: %d", msg, g.circleQueue.Len())
				} else { //存磁盘
					data, err := proto.Marshal(msg)
					if err != nil {
						myLogger.Logger.PrintError("Marshal :", err)
					}
					err = g.diskQueue1.StoreData(data)
					if err != nil {
						myLogger.Logger.PrintError("storeData :", err)
					}
					myLogger.Logger.Printf("push Msg %s to diskQueue, present Len: %d", msg, g.diskQueue1.msgNum)
				}
			} else { //高优先级
				if g.priorityQueue.Len() < g.queueSize { //存优先级队列
					heap.Push(g.priorityQueue, msg)
					myLogger.Logger.Printf("push Msg %s to priorityQueue, present Len: %d", msg, g.priorityQueue.Len())
				} else {
					myLogger.Logger.PrintError("should not come here")
				}
			}
		case <-retryTickerChan: //时间到，检测是否需要重传消息
			msg, err := g.circleQueue.Peek()
			if err != nil {
				myLogger.Logger.PrintError(err)
			}
			if msg.Timeout < time.Now().UnixNano() { //超时了
				g.circleQueue.ResetSendData() //将发送位移重置为队首元素
				break
			}
		case <-showQueueChan: //时间到，检测是否需要重传消息
			myLogger.Logger.PrintfDebug("分区名: %s  内存队列大小: %d",
				g.partitionName, g.circleQueue.Len())
		}

	}
exit:
	myLogger.Logger.Print("readLoop exit")
	if needReSend {
		retryTicker.Stop()
	}
	if showQueueSize {
		showQueueTicker.Stop()
	}
}

//func (g *subscribedGroup) pushInflightMsg(msg *protocol.InternalMessage) error{
//	msg.Timeout = time.Now().Add(retryTime).UnixNano() //设置超时时间
//	err := g.inflightQueue.Push(msg) //插入队列后面
//	if err != nil{
//		return err
//	}
//	myLogger.Logger.Printf("pushInflightMsg: %d  remain len: %d", msg.Id, g.inflightQueue.Len())
//
//	//heap.Push(g.inflightQueue, msg)
//	return nil
//}

//func (g *subscribedGroup) popInflightMsg(msgId int32) error{
//	for{
//		g.inflightQueue.
//		if !ok {
//			myLogger.Logger.Print("popInflightMsg not exist", msgId)
//			if
//		}
//		break
//	}
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
