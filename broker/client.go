package broker

import (
	"../mylib/myLogger"
	"../mylib/protocalFuc"
	"../protocol"
	"bufio"
	"github.com/golang/protobuf/proto"
	"io"
	"net"
	"sync"
	"time"
)

const defaultreadyNum = 1000
const writeimmediately = true

type client struct {
	id     int64
	conn   net.Conn
	reader *bufio.Reader
	//writerLock sync.RWMutex
	writer          *bufio.Writer
	broker          *Broker
	belongGroup     string
	consumePartions map[string]bool //该消费者消费的分区，key:分区名
	writeCmdChan    chan []byte
	writeMsgChan    chan []byte
	exitChan        chan string
	readyNum        int32      //客户端目前可接受的数据量
	changeReadyNum  chan int32 //使得readyNum只在writeLoop协程中修改
	//isbrokerExitLock1 sync.RWMutex
	isbrokerExit bool
}

//新建client实例
func NewClient(conn net.Conn, broker *Broker) *client {
	c := &client{
		id:              broker.GenerateClientId(),
		conn:            conn,
		reader:          bufio.NewReader(conn),
		writer:          bufio.NewWriter(conn),
		broker:          broker,
		consumePartions: make(map[string]bool),
		writeCmdChan:    make(chan []byte),
		writeMsgChan:    make(chan []byte),
		exitChan:        make(chan string),
		changeReadyNum:  make(chan int32),
		readyNum:        defaultreadyNum, //客户端默认可接收数据为1000
	}
	//broker.clientChangeChan <- &clientChange{true, c}
	waitFinished := make(chan bool)
	c.broker.clientChangeChan <- &clientChange{true, c, waitFinished} //放到broker 的readLoop协程中进行处理
	<-waitFinished                                                    //等待添加
	//broker.addClient(c)
	return c
}

//client处理函数
func (c *client) ClientHandle() {
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		c.readLoop()
		wg.Done()
	}()
	go func() {
		c.writeLoop()
		wg.Done()
	}()
	wg.Wait()
	myLogger.Logger.Print("a client leave1")
	c.clientExit()
	myLogger.Logger.Print("a client leave2")
}

func (c *client) getWriteMsgChan() chan []byte {
	return c.writeMsgChan
}

func (c *client) getWriteCmdChan() chan []byte {
	return c.writeCmdChan
}

//退出前回收数据
func (c *client) clientExit() {

	myLogger.Logger.Print("exit client :", c.id)
	waitFinished := make(chan bool)
	c.broker.clientChangeChan <- &clientChange{false, c, waitFinished} //放到broker 的readLoop协程中进行处理，避免频繁使用锁
	<-waitFinished                                                     //等待移除
	//从group中删除
	group, ok := c.broker.getGroup(&c.belongGroup)
	if !ok {
		myLogger.Logger.Printf("exit client %d do not belong to any group", c.id)
	} else {
		myLogger.Logger.Printf("exit client belong to group : %s", c.belongGroup)
		succ := group.deleteClient(c.id) //从group中删除
		if succ {
			if !c.isbrokerExit { //如果是broker退出了不用做负载均衡操作。
				group.Rebalance()
			}
		}
	}

	if !c.isbrokerExit { //如果是broker退出了说明conn已经关闭了
		c.broker.partitionMapLock.RLock()
		//从partition中删除
		for partitionName, _ := range c.consumePartions { //delete from partition
			partition, ok := c.broker.getPartition(&partitionName)
			if !ok {
				myLogger.Logger.Print("exit partition do not exist ：", partition.name)
			} else {
				myLogger.Logger.Print("exit partition : ", partition.name)
				partition.invalidComsummerClient(c, c.belongGroup)
			}
		}
		c.broker.partitionMapLock.RUnlock()
		c.conn.Close()
	}
	close(c.writeMsgChan)
	close(c.writeCmdChan)
	close(c.changeReadyNum)
	close(c.exitChan)
}

//负责写数据到客户端的协程
func (c *client) writeLoop() {
	var server2ClientData []byte
	writeMsgChan := c.writeMsgChan
	timeTicker := time.NewTicker(200 * time.Millisecond) //每200m秒定时触发一次
	for {
		select {
		case s := <-c.exitChan:
			myLogger.Logger.Print(s)
			goto exit
		case num := <-c.changeReadyNum:
			if num == -1 {
				c.readyNum++
				myLogger.Logger.Print("add readyNum: ", c.readyNum)
			} else {
				c.readyNum = num
				myLogger.Logger.Print("change readyNum: ", c.readyNum)
			}
			if c.readyNum <= 0 {
				//writeMsgChan =  nil//不可以往客户端写
			} else {
				writeMsgChan = c.writeMsgChan //可以开始往客户端写
			}
		case server2ClientData = <-writeMsgChan: //推送消息
			c.readyNum--
			if c.readyNum <= 0 {
				myLogger.Logger.Print("client not ready")
				//writeMsgChan = nil //不能再发消息了，除非client重现提交readycount
			}
			myLogger.Logger.Printf("writeMsgChan %s", server2ClientData)

			_, err := c.writer.Write(server2ClientData)
			if err != nil {
				myLogger.Logger.PrintError("writer error: ", err)
				continue
			}
			if writeimmediately {
				err = c.writer.Flush()
				if err != nil {
					myLogger.Logger.PrintError(err)
				}
			}
		case <-timeTicker.C: //定时也flush
			err := c.writer.Flush()
			if err != nil {
				myLogger.Logger.PrintError(err)
			}
		case server2ClientData = <-c.writeCmdChan: //推送命令
			//myLogger.Logger.Printf("writeResponse %s", server2ClientData)
			_, err := c.writer.Write(server2ClientData)
			if err != nil {
				myLogger.Logger.PrintError("writer error: ", err)
				continue
			}
			err = c.writer.Flush()
			if err != nil {
				myLogger.Logger.PrintError("writer error: ", err)
				continue
			}
		}
	}
exit:
	timeTicker.Stop()
	//err := c.writer.Flush()
	//if err != nil {
	//	myLogger.Logger.PrintError("writer error: ", err)
	//}
	return
}

//读客户端发来的消息的
func (c *client) readLoop() {
	for {
		myLogger.Logger.Print("readLoop")
		cmd, msgBody, err := protocalFuc.ReadAndUnPackClientServerProtoBuf(c.reader)
		if err != nil {
			if err == io.EOF {
				myLogger.Logger.Print("EOF")
			} else {
				myLogger.Logger.Print(err)
			}
			c.exitChan <- "bye"
			break
		}
		var response []byte
		switch *cmd {
		case protocol.ClientServerCmd_CmdCreatTopicReq:
			creatTopicReq := &protocol.CreatTopicReq{}
			err = proto.Unmarshal(msgBody, creatTopicReq) //得到包头
			if err != nil {
				myLogger.Logger.PrintError("Unmarshal error %s", err)
				continue
			} else {
				myLogger.Logger.Printf("receive creatTopicReq: %s", creatTopicReq)
			}
			response = c.broker.CreatTopic(creatTopicReq.TopicName, creatTopicReq.PartitionNum)
		case protocol.ClientServerCmd_CmdDeleteTopicReq:
			deleteTopicReq := &protocol.DeleteTopicReq{}
			err = proto.Unmarshal(msgBody, deleteTopicReq) //得到包头
			if err != nil {
				myLogger.Logger.PrintError("Unmarshal error %s", err)
				continue
			} else {
				myLogger.Logger.Printf("receive DeleteTopicReq: %s", deleteTopicReq)
			}
			response = c.broker.DeleteTopic(deleteTopicReq.TopicName)
		case protocol.ClientServerCmd_CmdGetPublisherPartitionReq:
			req := &protocol.GetPublisherPartitionReq{}
			err = proto.Unmarshal(msgBody, req) //得到消息体
			if err != nil {
				myLogger.Logger.PrintError("Unmarshal error %s", err)
				continue
			} else {
				myLogger.Logger.Printf("receive GetPublisherPartitionReq: %s", req)
			}
			response = c.broker.GetPublisherPartition(req.TopicName)
		case protocol.ClientServerCmd_CmdGetConsumerPartitionReq:
			req := &protocol.GetConsumerPartitionReq{}
			err = proto.Unmarshal(msgBody, req) //得到消息体
			if err != nil {
				myLogger.Logger.PrintError("Unmarshal error %s", err)
				continue
			} else {
				myLogger.Logger.Printf("receive GetConsumerPartitionReq: %s", req)
			}
			response = c.broker.GetConsumerPartition(req.GroupName, c.id)
		case protocol.ClientServerCmd_CmdSubscribePartitionReq:
			req := &protocol.SubscribePartitionReq{}
			err = proto.Unmarshal(msgBody, req) //得到消息体
			if err != nil {
				myLogger.Logger.PrintError("Unmarshal error %s", err)
				continue
			} else {
				myLogger.Logger.Printf("receive SubscribePartitionReq: %s", req)
			}
			response = c.broker.SubscribePartition(req.PartitionName, req.GroupName, c.id, req.RebalanceId)
		case protocol.ClientServerCmd_CmdSubscribeTopicReq:
			req := &protocol.SubscribeTopicReq{}
			err = proto.Unmarshal(msgBody, req) //得到消息体
			if err != nil {
				myLogger.Logger.PrintError("Unmarshal error %s", err)
				continue
			} else {
				myLogger.Logger.Printf("receive SubscribeTopicReq: %s", req)
			}
			response = c.broker.SubscribeTopic(req.TopicName, req.GroupName, c.id)
		case protocol.ClientServerCmd_CmdRegisterConsumerReq:
			req := &protocol.RegisterConsumerReq{}
			err = proto.Unmarshal(msgBody, req) //得到消息体
			if err != nil {
				myLogger.Logger.PrintError("Unmarshal error %s", err)
				continue
			} else {
				myLogger.Logger.Printf("receive RegisterConsumerReq: %s", req)
			}
			response = c.broker.RegisterConsumer(req.GroupName, c.id)
		case protocol.ClientServerCmd_CmdUnRegisterConsumerReq:
			req := &protocol.UnRegisterConsumerReq{}
			err = proto.Unmarshal(msgBody, req) //得到消息体
			if err != nil {
				myLogger.Logger.PrintError("Unmarshal error %s", err)
				continue
			} else {
				myLogger.Logger.Printf("receive UnRegisterConsumerReq: %s", req)
			}
			response = c.broker.UnRegisterConsumer(req.GroupName, c.id)
		case protocol.ClientServerCmd_CmdPublishReq:
			publishReq := &protocol.PublishReq{}
			err = proto.Unmarshal(msgBody, publishReq) //得到消息体
			if err != nil {
				myLogger.Logger.PrintError("Unmarshal error %s", err)
				continue
			} else {
				//myLogger.Logger.PrintfDebug2("Partition %s receive data: %s  priority: %d", publishReq.PartitionName, publishReq.Msg.Msg, publishReq.Msg.Priority)

				myLogger.Logger.PrintfDebug2("收到消息内容：%s   消息优先级：%d   发往分区：%s", publishReq.Msg.Msg, publishReq.Msg.Priority, publishReq.PartitionName)
				myLogger.Logger.Printf("receive PublishReq: %s", publishReq)
			}
			response = c.publish(publishReq.PartitionName, publishReq.Msg)
		case protocol.ClientServerCmd_CmdCommitReadyNumReq: //readyCount提交
			req := &protocol.CommitReadyNumReq{}
			err = proto.Unmarshal(msgBody, req) //得到消息体
			if err != nil {
				myLogger.Logger.PrintError("Unmarshal error %s", err)
				continue
			} else {
				myLogger.Logger.Printf("receive CommitReadyNumReq: %s", req)
			}
			c.changeReadyNum <- req.ReadyNum //修改readyCount
		case protocol.ClientServerCmd_CmdPublishWithoutAskReq:
			publishReq := &protocol.PublishReq{}
			err = proto.Unmarshal(msgBody, publishReq) //得到消息体
			if err != nil {
				myLogger.Logger.PrintError("Unmarshal error %s", err)
				continue
			} else {
				myLogger.Logger.Printf("receive PublishWithoutAskReq: %s", publishReq)
			}
			c.publish(publishReq.PartitionName, publishReq.Msg)
		case protocol.ClientServerCmd_CmdPushMsgRsp:
			//c.changeReadyNum <- -1 //修改readyCount++,如果客户端发送了一个ask消息，则客户端目前可接受的数据量readyCount应该加1,用-1表示加1
			rsp := &protocol.PushMsgRsp{}
			err = proto.Unmarshal(msgBody, rsp) //得到消息体
			if err != nil {
				myLogger.Logger.PrintError("Unmarshal error %s", err)
				continue
			} else {
				myLogger.Logger.Printf("receive PushMsgRsp: %s", rsp)
			}
			response = c.consumeSuccess(rsp.PartitionName, rsp.GroupName, rsp.MsgId, rsp.MsgPriority)
		default:
			myLogger.Logger.Print("cannot find key")
			//c.broker.readChan <- &readData{c.id, client2ServerData}//对于其它类型的消息，大多是修改或获取集群拓扑结构等，统一交给broker处理，减少锁的使用
		}
		if response != nil {
			c.writeCmdChan <- response //回写response
		}

	}
}

//处理消费成功的commit
func (c *client) consumeSuccess(partitionName string, groupName string, msgId int32, priority int32) (response []byte) {
	partition, ok := c.broker.getPartitionAndLock(&partitionName)
	defer c.broker.getPartitionUnlock()
	if !ok {
		myLogger.Logger.Printf("consume Partition Not existed : %s", partitionName)
		return nil
	} else {
		isPriorityMsg := false
		if priority > 0 {
			isPriorityMsg = true
		}
		msgAskData := &msgAskData{
			msgId:         msgId,
			groupName:     groupName,
			isPriorityMsg: isPriorityMsg,
		}
		partition.msgAskChan <- msgAskData
		return nil
	}
}

//处理client发布的消息
func (c *client) publish(partitionName string, msg *protocol.Message) (response []byte) {
	partition, ok := c.broker.getPartitionAndLock(&partitionName)
	defer c.broker.getPartitionUnlock()
	if !ok {
		myLogger.Logger.Printf("Partition Not existed : %s", partitionName)

		rsp := &protocol.PushMsgRsp{
			Ret: protocol.RetStatus_Fail,
		}
		data, err := proto.Marshal(rsp)
		if err != nil {
			myLogger.Logger.PrintError("marshaling error", err)
			return nil
		}
		reqData, err := protocalFuc.PackClientServerProtoBuf(protocol.ClientServerCmd_CmdPushMsgRsp, data)
		if err != nil {
			myLogger.Logger.PrintError("marshaling error", err)
			return nil
		}
		myLogger.Logger.Printf("write: %s", rsp)
		response = reqData

		return response
	} else {
		myLogger.Logger.Printf("publish msg : %s", msg.String())
		response = partition.Put(msg) //投递到partition

		return response
	}

}

//
//func (c *client) sendResponse2(response *protocol.Server2Client)  error{
//	data, err := proto.Marshal(response)
//	myLogger.Logger.Print("send sendResponse len:", len(data), response)
//	if err != nil {
//		myLogger.Logger.Print("marshaling error: ", err)
//		return err
//	}
//	var buf [4]byte
//	bufs := buf[:]
//	binary.BigEndian.PutUint32(bufs, uint32(len(data)))
//	c.writerLock.Lock()
//	c.writer.Write(bufs)
//	c.writer.Write(data)
//	c.writer.Flush()
//	c.writerLock.Unlock()
//	return nil
//}
