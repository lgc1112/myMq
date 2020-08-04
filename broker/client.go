package broker

import (
	"../mylib/myLogger"
	"../mylib/protocalFuc"
	"../protocol"
	"bufio"
	"encoding/binary"
	"github.com/golang/protobuf/proto"
	"io"
	"net"
	"sync"
)
const defaultreadyNum = 1000
type client struct {
	id int64
	conn net.Conn
	reader *bufio.Reader
	//writerLock sync.RWMutex
	writer *bufio.Writer
	broker *Broker
	belongGroup string
	consumePartions map[string] bool //该消费者消费的分区，key:分区名
	writeCmdChan     chan []byte
	writeMsgChan     chan []byte
	exitChan chan string
	readyNum int32 //客户端目前可接受的数据量
	changeReadyNum chan int32 //使得readyNum只在writeLoop协程中修改
	//isbrokerExitLock1 sync.RWMutex
	isbrokerExit bool
}

func newClient(conn net.Conn, broker *Broker)  *client{
	c := &client{
		id : broker.GenerateClientId(),
		conn: conn,
		reader: bufio.NewReader(conn),
		writer: bufio.NewWriter(conn),
		broker: broker,
		consumePartions: make(map[string] bool),
		writeCmdChan: make(chan []byte),
		writeMsgChan: make(chan []byte),
		exitChan: make(chan string),
		changeReadyNum: make(chan int32),
		readyNum: defaultreadyNum, //客户端默认可接收数据为1000
	}
	//broker.clientChangeChan <- &clientChange{true, c}
	waitFinished := make(chan bool)
	c.broker.clientChangeChan <- &clientChange{true, c, waitFinished}//放到broker 的readLoop协程中进行处理，避免频繁使用锁
	<- waitFinished //等待添加
	//broker.addClient(c)
	return c
}

//func (c *client)Close() {
//	c.conn.Close()
//}

func (c *client)clientHandle() {
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


func (c *client)clientExit() {
	//c.isbrokerExitLock1.RLock()
	if c.isbrokerExit{//如果是broker退出了就直接回收退出即可，不用做负载均衡删除client等操作。
		myLogger.Logger.Print("exit client isbrokerExit:", c.id)
		waitFinished := make(chan bool)
		c.broker.clientChangeChan <- &clientChange{false, c, waitFinished}//放到broker 的readLoop协程中进行处理，避免频繁使用锁
		<- waitFinished //等待移除

		close(c.writeMsgChan)
		close(c.writeCmdChan)
		close(c.changeReadyNum)
		close(c.exitChan)
		//c.isbrokerExitLock1.RUnlock()
		return
	}
	//c.isbrokerExitLock1.RUnlock()


	myLogger.Logger.Print("exit client :", c.id)
	waitFinished := make(chan bool)
	c.broker.clientChangeChan <- &clientChange{false, c, waitFinished}//放到broker 的readLoop协程中进行处理，避免频繁使用锁
	<- waitFinished //等待移除
	//c.broker.removeClient(c.id)
	//从group中删除
	group, ok := c.broker.getGroup(&c.belongGroup)
	if !ok {
		myLogger.Logger.Printf("exit client %d do not belong to any group", c.id)
	}else{
		myLogger.Logger.Printf("exit client belong to group : %s", c.belongGroup)
		succ:= group.deleteClient(c.id) //从group中删除
		if succ {
			if !c.isbrokerExit { //如果是broker退出了不用做负载均衡操作。
				group.Rebalance()
			}
		}
	}

	c.broker.partitionMapLock.RLock()
	//从partition中删除
	for partitionName, _ := range c.consumePartions{//delete from partition
		partition, ok := c.broker.getPartition(&partitionName)
		if !ok {
			myLogger.Logger.Print("exit partition do not exist ：", partition.name)
		}else{
			myLogger.Logger.Print("exit partition : ", partition.name)
			partition.invalidComsummerClient(c, c.belongGroup)
		}
	}
	c.broker.partitionMapLock.RUnlock()

	if !c.isbrokerExit { //如果是broker退出了说明conn已经关闭了
		c.conn.Close()
	}
	close(c.writeMsgChan)
	close(c.writeCmdChan)
	close(c.changeReadyNum)
	close(c.exitChan)
}


func (c *client) writeLoop() {
	var server2ClientData []byte
	writeMsgChan := c.writeMsgChan
	for{
		select {
		case s := <- c.exitChan:
			myLogger.Logger.Print(s)
			goto exit
		case num := <- c.changeReadyNum:
			if num == -1{
				c.readyNum++
				myLogger.Logger.Print("add readyNum: ", c.readyNum)
			}else{
				c.readyNum = num
				myLogger.Logger.Print("change readyNum: ", c.readyNum)
			}
			if c.readyNum <= 0{
				//writeMsgChan =  nil//不可以往客户端写
			}else{
				writeMsgChan =  c.writeMsgChan//可以开始往客户端写
			}
		case server2ClientData = <- writeMsgChan://如果向客户端发送了一条消息，则客户端目前可接受的数据量readyCount应该减一
			c.readyNum--
			if c.readyNum <= 0{
				myLogger.Logger.Print("client not ready")
				//writeMsgChan = nil //不能再发消息了，除非client重现提交readycount
			}
			myLogger.Logger.Printf("writeMsgChan %s", server2ClientData)
			//data, err := proto.Marshal(server2ClientData)
			////myLogger.Logger.Print("send sendResponse len:", len(data), response)
			//if err != nil {
			//	myLogger.Logger.PrintError("marshaling error: ", err)
			//	continue
			//}
			//var buf [4]byte
			//bufs := buf[:]
			//binary.BigEndian.PutUint32(bufs, uint32(len(data)))
			////c.writerLock.Lock()
			//_, err = c.writer.Write(bufs)
			//if err != nil {
			//	myLogger.Logger.PrintError("writer error: ", err)
			//	continue
			//}

			//var buf [4]byte
			bufs :=  make([]byte, 4)
			binary.BigEndian.PutUint32(bufs, uint32(len(server2ClientData)))
			_, err := c.writer.Write(bufs)
			if err != nil {
				myLogger.Logger.PrintError("writer error: ", err)
				continue
			}

			_, err = c.writer.Write(server2ClientData)
			if err != nil {
				myLogger.Logger.PrintError("writer error: ", err)
				continue
			}
			err = c.writer.Flush()
			if err != nil {
				myLogger.Logger.PrintError("writer error: ", err)
				continue
			}
			//c.writerLock.Unlock()
			//c.sendResponse(response)

		case server2ClientData = <- c.writeCmdChan:
			myLogger.Logger.Printf("writeResponse %s", server2ClientData)
			//data, err := proto.Marshal(server2ClientData)
			////myLogger.Logger.Print("send sendResponse len:", len(data), response)
			//if err != nil {
			//	myLogger.Logger.PrintError("marshaling error: ", err)
			//	continue
			//}
			//var buf [4]byte
			//bufs := buf[:]
			//binary.BigEndian.PutUint32(bufs, uint32(len(data)))
			////c.writerLock.Lock()
			//_, err = c.writer.Write(bufs)
			//if err != nil {
			//	myLogger.Logger.PrintError("writer error: ", err)
			//	continue
			//}

			//var buf [4]byte
			bufs :=  make([]byte, 4)
			binary.BigEndian.PutUint32(bufs, uint32(len(server2ClientData)))
			_, err := c.writer.Write(bufs)
			if err != nil {
				myLogger.Logger.PrintError("writer error: ", err)
				continue
			}
			_, err = c.writer.Write(server2ClientData)
			if err != nil {
				myLogger.Logger.PrintError("writer error: ", err)
				continue
			}
			err = c.writer.Flush()
			if err != nil {
				myLogger.Logger.PrintError("writer error: ", err)
				continue
			}
			//c.writerLock.Unlock()
			//c.sendResponse(response)
		}
	}
exit:
	//myLogger.Logger.Print("close writeLoop")
	return
}
//
//func (c *client)readLoop() {
//	for{
//		myLogger.Logger.Print("readLoop")
//		tmp := make([]byte, 4)
//		_, err := io.ReadFull(c.reader, tmp) //读取长度
//		if err != nil {
//			if err == io.EOF {
//				myLogger.Logger.Print("EOF")
//			} else {
//				myLogger.Logger.Print(err)
//			}
//			c.exitChan <- "bye"
//			break
//		}
//		len := int32(binary.BigEndian.Uint32(tmp))
//		myLogger.Logger.Printf("readLen %d ", len)
//		requestData := make([]byte, len)
//		_, err = io.ReadFull(c.reader, requestData) //读取内容
//		if err != nil {
//			if err == io.EOF {
//				myLogger.Logger.Print("EOF")
//			} else {
//				myLogger.Logger.Print(err)
//			}
//			c.exitChan <- "bye"
//			break
//		}
//		clientServerHeader := &protocol.ClientServerHeader{} //读消息头
//		err = proto.Unmarshal(requestData, clientServerHeader)
//		if err != nil {
//			myLogger.Logger.PrintError("Unmarshal error %s", err)
//			break
//		}else{
//			myLogger.Logger.Printf("receive client2ServerData: %s", clientServerHeader)
//		}
//
//
//
//
//
//		client2ServerData := &protocol.Client2Server{}
//		err = proto.Unmarshal(requestData, client2ServerData)
//		if err != nil {
//			myLogger.Logger.PrintError("Unmarshal error %s", err)
//		}else{
//			myLogger.Logger.Printf("receive client2ServerData: %s", client2ServerData)
//		}
//		var response *protocol.Server2Client
//		switch client2ServerData.Key {
//		case protocol.Client2ServerKey_CreatTopic:
//			response = c.broker.creatTopic(client2ServerData.Topic, client2ServerData.PartitionNum)
//		case protocol.Client2ServerKey_DeleteTopic:
//			response = c.broker.deleteTopic2(client2ServerData.Topic)
//		case protocol.Client2ServerKey_GetPublisherPartition:
//			response = c.broker.getPublisherPartition(client2ServerData.Topic)
//		case protocol.Client2ServerKey_GetConsumerPartition:
//			response = c.broker.getConsumerPartition(client2ServerData.GroupName, c.id)
//		case protocol.Client2ServerKey_SubscribePartion:
//			response = c.broker.subscribePartition(client2ServerData.Partition, client2ServerData.GroupName, c.id, client2ServerData.RebalanceId)
//		case protocol.Client2ServerKey_SubscribeTopic:
//			response = c.broker.subscribeTopic(client2ServerData.Topic, client2ServerData.GroupName, c.id)
//		case protocol.Client2ServerKey_RegisterConsumer:
//			response = c.broker.registerConsumer(client2ServerData.GroupName, c.id)
//		case protocol.Client2ServerKey_UnRegisterConsumer:
//			response = c.broker.unRegisterConsumer(client2ServerData.GroupName, c.id)
//		case protocol.Client2ServerKey_Publish:
//			response = c.publish(client2ServerData.Partition, client2ServerData.Msg)
//			//continue
//		case protocol.Client2ServerKey_CommitReadyNum://readyCount提交
//			c.changeReadyNum <- client2ServerData.ReadyNum //修改readyCount
//			//continue
//		case protocol.Client2ServerKey_ConsumeSuccess:
//			c.changeReadyNum <- -1 //修改readyCount++,如果客户端发送了一个ask消息，则客户端目前可接受的数据量readyCount应该加1,用-1表示加1
//			response = c.consumeSuccess(client2ServerData.Partition, client2ServerData.GroupName, client2ServerData.MsgId)
//			//continue
//		default:
//			myLogger.Logger.Print("cannot find key")
//			//c.broker.readChan <- &readData{c.id, client2ServerData}//对于其它类型的消息，大多是修改或获取集群拓扑结构等，统一交给broker处理，减少锁的使用
//		}
//
//		if response != nil{
//			c.writeCmdChan <- response
//		}
//
//	}
//}
//
func (c *client)readLoop() {
	for{
		myLogger.Logger.Print("readLoop")
		//tmp := make([]byte, 4)
		//_, err := io.ReadFull(c.reader, tmp) //读取长度
		//if err != nil {
		//	if err == io.EOF {
		//		myLogger.Logger.Print("EOF")
		//	} else {
		//		myLogger.Logger.Print(err)
		//	}
		//	c.exitChan <- "bye"
		//	break
		//}
		//len := int32(binary.BigEndian.Uint32(tmp))
		//myLogger.Logger.Printf("readLen %d ", len)
		//requestData := make([]byte, len)
		//_, err = io.ReadFull(c.reader, requestData) //读取内容
		//if err != nil {
		//	if err == io.EOF {
		//		myLogger.Logger.Print("EOF")
		//	} else {
		//		myLogger.Logger.Print(err)
		//	}
		//	c.exitChan <- "bye"
		//	break
		//}
		//client2ServerData := &protocol.Client2Server{}
		//err = proto.Unmarshal(requestData, client2ServerData)
		//if err != nil {
		//	myLogger.Logger.PrintError("Unmarshal error %s", err)
		//}else{
		//	myLogger.Logger.Printf("receive client2ServerData: %s", client2ServerData)
		//}

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
			}else{
				myLogger.Logger.Printf("receive creatTopicReq: %s", creatTopicReq)
			}
			response = c.broker.creatTopic(creatTopicReq.TopicName, creatTopicReq.PartitionNum)
		case protocol.ClientServerCmd_CmdDeleteTopicReq:
			deleteTopicReq := &protocol.DeleteTopicReq{}
			err = proto.Unmarshal(msgBody, deleteTopicReq) //得到包头
			if err != nil {
				myLogger.Logger.PrintError("Unmarshal error %s", err)
				continue
			}else{
				myLogger.Logger.Printf("receive creatTopicReq: %s", deleteTopicReq)
			}
			response = c.broker.deleteTopic2(deleteTopicReq.TopicName)
		case protocol.ClientServerCmd_CmdGetPublisherPartitionReq:
			req := &protocol.GetPublisherPartitionReq{}
			err = proto.Unmarshal(msgBody, req) //得到消息体
			if err != nil {
				myLogger.Logger.PrintError("Unmarshal error %s", err)
				continue
			}else{
				myLogger.Logger.Printf("receive GetPublisherPartitionReq: %s", req)
			}
			response = c.broker.getPublisherPartition(req.TopicName)
		case protocol.ClientServerCmd_CmdGetConsumerPartitionReq:
			req := &protocol.GetConsumerPartitionReq{}
			err = proto.Unmarshal(msgBody, req) //得到消息体
			if err != nil {
				myLogger.Logger.PrintError("Unmarshal error %s", err)
				continue
			}else{
				myLogger.Logger.Printf("receive GetConsumerPartitionReq: %s", req)
			}
			response = c.broker.getConsumerPartition(req.GroupName, c.id)
		case protocol.ClientServerCmd_CmdSubscribePartitionReq:
			req := &protocol.SubscribePartitionReq{}
			err = proto.Unmarshal(msgBody, req) //得到消息体
			if err != nil {
				myLogger.Logger.PrintError("Unmarshal error %s", err)
				continue
			}else{
				myLogger.Logger.Printf("receive SubscribePartitionReq: %s", req)
			}
			response = c.broker.subscribePartition(req.PartitionName, req.GroupName, c.id, req.RebalanceId)
		case protocol.ClientServerCmd_CmdSubscribeTopicReq:
			req := &protocol.SubscribeTopicReq{}
			err = proto.Unmarshal(msgBody, req) //得到消息体
			if err != nil {
				myLogger.Logger.PrintError("Unmarshal error %s", err)
				continue
			}else{
				myLogger.Logger.Printf("receive SubscribeTopicReq: %s", req)
			}
			response = c.broker.subscribeTopic(req.TopicName, req.GroupName, c.id)
		case protocol.ClientServerCmd_CmdRegisterConsumerReq:
			req := &protocol.RegisterConsumerReq{}
			err = proto.Unmarshal(msgBody, req) //得到消息体
			if err != nil {
				myLogger.Logger.PrintError("Unmarshal error %s", err)
				continue
			}else{
				myLogger.Logger.Printf("receive RegisterConsumerReq: %s", req)
			}
			response = c.broker.registerConsumer(req.GroupName, c.id)
		case protocol.ClientServerCmd_CmdUnRegisterConsumerReq:
			req := &protocol.UnRegisterConsumerReq{}
			err = proto.Unmarshal(msgBody, req) //得到消息体
			if err != nil {
				myLogger.Logger.PrintError("Unmarshal error %s", err)
				continue
			}else{
				myLogger.Logger.Printf("receive UnRegisterConsumerReq: %s", req)
			}
			response = c.broker.unRegisterConsumer(req.GroupName, c.id)
		case protocol.ClientServerCmd_CmdPublishReq:
			publishReq := &protocol.PublishReq{}
			err = proto.Unmarshal(msgBody, publishReq) //得到消息体
			if err != nil {
				myLogger.Logger.PrintError("Unmarshal error %s", err)
				continue
			}else{
				myLogger.Logger.Printf("receive PublishReq: %s", publishReq)
			}
			response = c.publish(publishReq.PartitionName, publishReq.Msg)
		case protocol.ClientServerCmd_CmdCommitReadyNumReq://readyCount提交
			req := &protocol.CommitReadyNumReq{}
			err = proto.Unmarshal(msgBody, req) //得到消息体
			if err != nil {
				myLogger.Logger.PrintError("Unmarshal error %s", err)
				continue
			}else{
				myLogger.Logger.Printf("receive CommitReadyNumReq: %s", req)
			}
			c.changeReadyNum <- req.ReadyNum //修改readyCount
		case protocol.ClientServerCmd_CmdPushMsgRsp:
			c.changeReadyNum <- -1 //修改readyCount++,如果客户端发送了一个ask消息，则客户端目前可接受的数据量readyCount应该加1,用-1表示加1

			rsp := &protocol.PushMsgRsp{}
			err = proto.Unmarshal(msgBody, rsp) //得到消息体
			if err != nil {
				myLogger.Logger.PrintError("Unmarshal error %s", err)
				continue
			}else{
				myLogger.Logger.Printf("receive PushMsgRsp: %s", rsp)
			}
			response = c.consumeSuccess(rsp.PartitionName, rsp.GroupName, rsp.MsgId)
		default:
			myLogger.Logger.Print("cannot find key")
			//c.broker.readChan <- &readData{c.id, client2ServerData}//对于其它类型的消息，大多是修改或获取集群拓扑结构等，统一交给broker处理，减少锁的使用
		}
		if response != nil{
			c.writeCmdChan <- response
		}


		//mes, err := protocalFuc.UnPackClientServer(c.reader)
		//if err != nil {
		//	if err == io.EOF {
		//		myLogger.Logger.Print("EOF")
		//	} else {
		//		myLogger.Logger.Print(err)
		//	}
		//	c.exitChan <- "bye"
		//	break
		//}
		//client2ServerData := mes.(*protocol.Client2Server)
		//var response []byte
		//switch client2ServerData.Key {
		//case protocol.Client2ServerKey_CreatTopic:
		//	response = c.broker.creatTopic(client2ServerData.Topic, client2ServerData.PartitionNum)
		//case protocol.Client2ServerKey_DeleteTopic:
		//	response = c.broker.deleteTopic2(client2ServerData.Topic)
		//case protocol.Client2ServerKey_GetPublisherPartition:
		//	response = c.broker.getPublisherPartition(client2ServerData.Topic)
		//case protocol.Client2ServerKey_GetConsumerPartition:
		//	response = c.broker.getConsumerPartition(client2ServerData.GroupName, c.id)
		//case protocol.Client2ServerKey_SubscribePartion:
		//	response = c.broker.subscribePartition(client2ServerData.Partition, client2ServerData.GroupName, c.id, client2ServerData.RebalanceId)
		//case protocol.Client2ServerKey_SubscribeTopic:
		//	response = c.broker.subscribeTopic(client2ServerData.Topic, client2ServerData.GroupName, c.id)
		//case protocol.Client2ServerKey_RegisterConsumer:
		//	response = c.broker.registerConsumer(client2ServerData.GroupName, c.id)
		//case protocol.Client2ServerKey_UnRegisterConsumer:
		//	response = c.broker.unRegisterConsumer(client2ServerData.GroupName, c.id)
		//case protocol.Client2ServerKey_Publish:
		//	response = c.publish(client2ServerData.Partition, client2ServerData.Msg)
		//	//continue
		//case protocol.Client2ServerKey_CommitReadyNum://readyCount提交
		//	c.changeReadyNum <- client2ServerData.ReadyNum //修改readyCount
		//	//continue
		//case protocol.Client2ServerKey_ConsumeSuccess:
		//	c.changeReadyNum <- -1 //修改readyCount++,如果客户端发送了一个ask消息，则客户端目前可接受的数据量readyCount应该加1,用-1表示加1
		//	response = c.consumeSuccess(client2ServerData.Partition, client2ServerData.GroupName, client2ServerData.MsgId)
		//	//continue
		//default:
		//	myLogger.Logger.Print("cannot find key")
		//	//c.broker.readChan <- &readData{c.id, client2ServerData}//对于其它类型的消息，大多是修改或获取集群拓扑结构等，统一交给broker处理，减少锁的使用
		//}
		//
		//if response != nil{
		//	c.writeCmdChan <- response
		//}

	}
}

func (c *client)  consumeSuccess(partitionName string, groupName string, msgId int32)   (response []byte) {
	//partitionName := client2ServerData.Partition

	c.broker.partitionMapLock.RLock()
	defer c.broker.partitionMapLock.RUnlock()

	partition, ok := c.broker.getPartition(&partitionName)
	if !ok {
		myLogger.Logger.Printf("Partition Not existed : %s", partitionName)
		Server2Client := &protocol.Server2Client{
			Key: protocol.Server2ClientKey_TopicNotExisted,
		}
		response, err := proto.Marshal(Server2Client)
		if err != nil {
			myLogger.Logger.PrintError("marshaling error: ", err)
			return nil
		}
		return response
	}else {
		//myLogger.Logger.Printf("publish msg : %s", msg.String())
		msgAskData := &msgAskData{
			msgId: msgId,
			groupName: groupName,
		}
		partition.msgAskChan <- msgAskData
		//response = &protocol.Server2Client{
		//	Key: protocol.Server2ClientKey_Success,
		//}
		return nil
	}
}

func (c *client)  publish(partitionName string, msg *protocol.Message)  (response []byte) {
	//partitionName := client2ServerData.Partition
	//msg := client2ServerData.Msg
	c.broker.partitionMapLock.RLock()
	defer c.broker.partitionMapLock.RUnlock()
	partition, ok := c.broker.getPartition(&partitionName)
	if !ok {
		myLogger.Logger.Printf("Partition Not existed : %s", partitionName)
		Server2Client := &protocol.Server2Client{
			Key: protocol.Server2ClientKey_TopicNotExisted,
		}

		response, err := proto.Marshal(Server2Client)
		if err != nil {
			myLogger.Logger.PrintError("marshaling error: ", err)
			return nil
		}
		return response
	}else{
		myLogger.Logger.Printf("publish msg : %s", msg.String())
		response = partition.Put(msg)
		//partition.msgChan <- msg
		//response = <- partition.responseChan
		//response = &protocol.Server2Client{
		//	Key: protocol.Server2ClientKey_Success,
		//}
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
