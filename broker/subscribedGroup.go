package broker

import (
	"../mylib/myLogger"
	"../protocol"
	"container/list"
	"errors"
	"github.com/golang/protobuf/proto"
	"sync"
	"time"
)

const queueSize = 1//队列大小
const retryTime = 1000 * time.Millisecond//队列重试时间
const MaxTryTime = 10 //最多重试的时间
type subscribedGroup struct {
	name string
	partitionName string
	wg sync.WaitGroup
	//consumerClientLock sync.RWMutex
	consumerClient *client //限制在IOloop进程中修改，不加锁
	circleQueue1 *circleQueue
	circleQueue2 *circleQueue //高优先级队列
	inflightQueue *list.List //链表

	inFlightMsgMap map[int32] *list.Element


	readChan chan *protocol.InternalMessage
	preparedMsgChan chan *protocol.InternalMessage
	clientChangeChan     chan *clientChange
	readLoopExitChan chan string
	writeLoopExitChan chan string
	//exitFinishedChan chan string
	msgAskChan     chan int32
	diskQueue1 *diskQueue
	diskQueue2 *diskQueue
}


func newPartionGroup(name, partitionName string) *subscribedGroup  {
	g := &subscribedGroup{
		readChan: make(chan *protocol.InternalMessage),
		preparedMsgChan: make(chan *protocol.InternalMessage),
		clientChangeChan: make(chan *clientChange),
		msgAskChan: make(chan int32),
		readLoopExitChan: make(chan string),
		writeLoopExitChan: make(chan string),
		inFlightMsgMap: make(map[int32] *list.Element),
		circleQueue1: NewCircleQueue(queueSize + 2),//开大两个字节，队列满关闭时才可以多存储内存中的两个数据
		circleQueue2: NewCircleQueue(queueSize + 2),
		inflightQueue: list.New(),
		partitionName: partitionName,
		name: name,
		diskQueue1: NewDiskQueue("./data/" + partitionName + "/" + name + "-p1/"),
		diskQueue2: NewDiskQueue("./data/" + partitionName + "/" + name + "-p2/"),
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
	for {//把inflight数据全部存入磁盘
		e := g.inflightQueue.Front()
		if e == nil{//读完了
			break
		}
		inflightMes := e.Value.(*protocol.InternalMessage)
		g.popInflightMsg(inflightMes.Id)

		data, err := proto.Marshal(inflightMes)
		if err != nil{
			myLogger.Logger.PrintError("Marshal :", err)
		}
		err = g.diskQueue1.storeData(data)
		if err != nil{
			myLogger.Logger.PrintError("storeData :", err)
		}
		myLogger.Logger.Printf("push Msg %s to diskQueue, present Len: %d", inflightMes, g.diskQueue1.msgNum)
	}
	for g.circleQueue1.Len() > 0{ //把优先级队列1的数据全部存入磁盘
		myLogger.Logger.Print("read Data from priorityQueue")
		circleQueue2Data, _ := g.circleQueue1.Pop()
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

	for g.circleQueue2.Len() > 0{ //把优先级队列2的数据全部存入磁盘
		myLogger.Logger.Print("read Data from priorityQueue")
		circleQueue2Data, _ := g.circleQueue2.Pop()
		data, err := proto.Marshal(circleQueue2Data)
		if err != nil{
			myLogger.Logger.PrintError("Marshal :", err)
		}
		err = g.diskQueue2.storeData(data)
		if err != nil{
			myLogger.Logger.PrintError("storeData :", err)
		}
		myLogger.Logger.Printf("push Msg %s to diskQueue, present Len: %d", circleQueue2Data, g.diskQueue2.msgNum)
	}



	err := g.diskQueue1.sync()
	if err != nil{
		myLogger.Logger.PrintError("persistDiskData1 :", err)
	}
	err = g.diskQueue2.sync()
	if err != nil{
		myLogger.Logger.PrintError("persistDiskData2 :", err)
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
	for{
		select {
		case <- g.writeLoopExitChan: //要退出了，保存好pushMsg中的临时数据，防止丢失
			if pushMsg != nil{ //
				g.pushInflightMsg(msg)
			}
			goto exit
		case tmp := <- g.clientChangeChan:
			if tmp.isAdd{//添加或修改client
				g.consumerClient = tmp.client
				consumerChan = g.consumerClient.writeMsgChan //可以往这边消费者写数据了
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
				if inflightMes.Timeout > time.Now().UnixNano() { //未超时，不处理
					break
				}
				//now:= time.Now()
				//myLogger.Logger.PrintDebug( inflightMes.Timeout , now.UnixNano())
				//inflightMes.Timeout = now.Add(retryTime).UnixNano() //设置超时时间
				//myLogger.Logger.PrintDebug( inflightMes.Timeout ,retryTime)
				if inflightMes.TryTimes > int32(MaxTryTime) {//重传太多次了，不传了，输出警告
					myLogger.Logger.PrintWarning("intflightQueue try too many times :", inflightMes.TryTimes)
					continue
				}
				myLogger.Logger.PrintDebug("intflightQueue mes time out", g.inflightQueue.Len())
				g.readChan <- inflightMes //此管道阻塞时，会有存磁盘操作，所以不会长时间阻塞，重新给group消费
				g.popInflightMsg(inflightMes.Id)
			}
		}
	}
exit:
	myLogger.Logger.Print("writeLoop exit")
	retryTicker.Stop()

}

func (g *subscribedGroup) readLoop()  {
	var msg *protocol.InternalMessage
	var memPreparedMsgChan chan *protocol.InternalMessage
	//var diskPreparedMsgChan chan *protocol.InternalMessage
	var diskReadyChan1 chan []byte
	var diskReadyChan2 chan []byte
	var memReadyData *protocol.InternalMessage
	var diskReadyData1 *protocol.InternalMessage
	var diskReadyData2 *protocol.InternalMessage
	//var haveProcessPreparedData bool = true
	for{

		//负责读disk1中的消息到circleQueue1
		if diskReadyData1 == nil{ //准备好的数据为空
			diskReadyChan1 = g.diskQueue1.readyChan //可从磁盘读数据
		}else{//磁盘数据准备好了
			if g.circleQueue1.Len() < queueSize{//放到内存队列
				g.circleQueue1.Push(diskReadyData1) //放到队列末尾
				myLogger.Logger.Printf("push Msg %s to circleQueue, present Len: %d", diskReadyData1, g.circleQueue1.Len())
				diskReadyData1 = nil//清空
				diskReadyChan1 = g.diskQueue1.readyChan //可从磁盘读新数据
			}else{
				diskReadyChan1 = nil //读出的数据未处理，不可从磁盘读数据,使其阻塞
			}
		}

		//负责读disk2中的消息到circleQueue2
		if diskReadyData2 == nil{ //准备好的数据为空
			diskReadyChan2 = g.diskQueue2.readyChan //可从磁盘读数据
		}else{//磁盘数据准备好了
			if g.circleQueue2.Len() < queueSize{//放到内存队列
				g.circleQueue2.Push(diskReadyData2) //放到队列末尾
				diskReadyData2 = nil//清空
				diskReadyChan2 = g.diskQueue2.readyChan //可从磁盘读新数据
				myLogger.Logger.Printf("push Msg %s to circleQueue, present Len: %d", msg, g.circleQueue1.Len())
			}else{
				diskReadyChan2 = nil //读出的数据未处理，不可从磁盘读数据,使其阻塞
			}
		}

		//读取内存中的消息，准备发送
		if memReadyData == nil && g.circleQueue2.Len() > 0{//优先队列中有数据必须先发完优先队列中的
			myLogger.Logger.Print("read Data from circleQueue2")
			memReadyData, _ = g.circleQueue2.Pop()
		}else if memReadyData == nil && g.circleQueue1.Len() > 0{ //只有优先队列中没有数据，才往这里读数据
			myLogger.Logger.Print("read Data from circleQueue1")
			memReadyData, _ = g.circleQueue1.Pop()
		}
		if memReadyData == nil{
			memPreparedMsgChan = nil //内存无数据可发，设为nil使其阻塞
		}else{
			memPreparedMsgChan = g.preparedMsgChan //内存有数据，管道准备好时应该发送
		}

		select {
		case <- g.readLoopExitChan: //要退出了，保存好这3个内存中的数据，防止丢失
			if diskReadyData1 != nil{
				g.circleQueue1.Push(diskReadyData1)//保证队列满存储也不出问题
			}
			if diskReadyData2 != nil{
				g.circleQueue2.Push(diskReadyData2)
			}
			if memReadyData != nil{ //
				g.circleQueue1.Push(memReadyData) //保证队列满存储也不出问题
			}
			goto exit
		case bytes := <- diskReadyChan1: //1有磁盘数据可读
			if diskReadyData1 != nil {
				myLogger.Logger.PrintError("Should not come here")
			}
			diskReadyData1 = &protocol.InternalMessage{}
			err := proto.Unmarshal(bytes, diskReadyData1)
			if err != nil{
				myLogger.Logger.PrintError("Unmarshal :", err)
			}

		case bytes := <- diskReadyChan2: //2有磁盘数据可读
			if diskReadyData2 != nil {
				myLogger.Logger.PrintError("Should not come here")
			}
			diskReadyData2 = &protocol.InternalMessage{}
			err := proto.Unmarshal(bytes, diskReadyData2)
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
			if msg.Priority == 0{
				if g.circleQueue1.Len() < queueSize{// && g.diskQueue1.msgNum == 0，只有磁盘中的数据已经处理完了并且内存数据为0才可以存内存，保证顺序性
					g.circleQueue1.Push(msg)
					//heap.Push(g.priorityQueue, msg)
					myLogger.Logger.Printf("push Msg %s to circleQueue1, present Len: %d", msg, g.circleQueue1.Len())
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
				if g.circleQueue2.Len() < queueSize{// && g.diskQueue2.msgNum == 0，只有磁盘中的数据已经处理完了并且内存数据为0才可以存内存，保证顺序性
					g.circleQueue2.Push(msg)
					//heap.Push(g.priorityQueue, msg)
					myLogger.Logger.Printf("push Msg %s to circleQueue2, present Len: %d", msg, g.circleQueue2.Len())
				}else{//存磁盘
					data, err := proto.Marshal(msg)
					if err != nil{
						myLogger.Logger.PrintError("Marshal :", err)
					}
					err = g.diskQueue2.storeData(data)
					if err != nil{
						myLogger.Logger.PrintError("storeData :", err)
					}
					myLogger.Logger.Printf("push Msg %s to diskQueue2, present Len: %d", msg, g.diskQueue2.msgNum)
				}
			}


		}
	}
	exit:
		myLogger.Logger.Print("readLoop exit")
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