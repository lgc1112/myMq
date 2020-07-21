package broker

import (
	"../mylib/myLogger"
	"../protocol"
	"bufio"
	"encoding/binary"
	"github.com/golang/protobuf/proto"
	"io"
	"net"
	"sync"
	"sync/atomic"
)
const writeChanSize int = 1000
var Logger *myLogger.MyLogger
type client struct {
	id int64
	conn net.Conn
	reader *bufio.Reader
	writerLock sync.RWMutex
	writer *bufio.Writer
	broker *Broker
	belongGroup string
	consumePartions map[string] bool
	writeChan     chan *protocol.Server2Client
	exitChan chan string
}

func newClient(conn net.Conn, broker *Broker)  *client{
	c := &client{
		id : atomic.AddInt64(&broker.maxClientId, 1),
		conn: conn,
		reader: bufio.NewReader(conn),
		writer: bufio.NewWriter(conn),
		broker: broker,
		consumePartions: make(map[string] bool),
		writeChan: make(chan *protocol.Server2Client, writeChanSize),
		exitChan: make(chan string),
	}
	broker.addClient(c)
	return c
}

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
	myLogger.Logger.Print("a client leave")
	c.clientExit()
}

func (c *client)clientExit() {
	myLogger.Logger.Print("exit client :", c.id)
	c.broker.removeClient(c)


	//从group中删除
	group, ok := c.broker.getGroup(&c.belongGroup)
	if !ok {
		myLogger.Logger.Print("exit client do not belong to any group")
	}else{
		myLogger.Logger.Printf("exit client belong to group : %s", c.belongGroup)
		succ:= group.deleteClient(c) //从group中删除
		if succ {
			group.rebalance()
		}
	}

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

	c.conn.Close()
	close(c.writeChan)
	close(c.exitChan)
}


func (c *client) writeLoop() {
	var response *protocol.Server2Client
	for{
		select {
		case s := <- c.exitChan:
			myLogger.Logger.Print(s)
			goto exit
		case response = <- c.writeChan:
			myLogger.Logger.Printf("writeResponse %s", response.String())
			c.sendResponse(response)
		}
	}
exit:
	//myLogger.Logger.Print("close writeLoop")
	return
}
func (c *client)readLoop() {
	for{
		myLogger.Logger.Print("readLoop");
		tmp := make([]byte, 4)
		_, err := io.ReadFull(c.reader, tmp) //读取长度
		if err != nil {
			if err == io.EOF {
				myLogger.Logger.Print("EOF")
			} else {
				myLogger.Logger.Print(err)
			}
			c.exitChan <- "bye"
			break
		}
		len := int32(binary.BigEndian.Uint32(tmp))
		myLogger.Logger.Printf("readLen %d ", len)
		requestData := make([]byte, len)
		_, err = io.ReadFull(c.reader, requestData) //读取内容
		if err != nil {
			if err == io.EOF {
				myLogger.Logger.Print("EOF")
			} else {
				myLogger.Logger.Print(err)
			}
			c.exitChan <- "bye"
			break
		}
		request := &protocol.Client2Server{}
		err = proto.Unmarshal(requestData, request)
		if err != nil {
			myLogger.Logger.Print("Unmarshal error %s", err)
		}else{
			myLogger.Logger.Printf("receive request: %s", request)
		}
		var response *protocol.Server2Client
		switch request.Key {
		case protocol.Client2ServerKey_CreatTopic:
			response = c.creatTopic(request)
		case protocol.Client2ServerKey_GetPublisherPartition:
			response = c.getPublisherPartition(request)
		case protocol.Client2ServerKey_Publish:
			response = c.publish(request)
		case protocol.Client2ServerKey_GetConsumerPartition:
			response = c.getConsumerPartition(request)
		case protocol.Client2ServerKey_SubscribePartion:
			response = c.subscribePartition(request)
		case protocol.Client2ServerKey_SubscribeTopic:
			response = c.subscribeTopic(request)
		case protocol.Client2ServerKey_RegisterConsumer:
			response = c.registerConsumer(request)
		case protocol.Client2ServerKey_UnRegisterConsumer:
			response = c.unRegisterConsumer(request)
		default:
			myLogger.Logger.Print("cannot find key");
		}
		if response != nil{
			c.sendResponse(response)
		}
	}
}

func (c *client) creatTopic(request *protocol.Client2Server)  (response *protocol.Server2Client) {
	topicName := request.Topic
	partionNum := request.PartitionNum
	topic, ok := c.broker.getTopic(&topicName)
	if ok {
		myLogger.Logger.Printf("try to create existed topic : %s %d", topicName, int(partionNum))
		response = &protocol.Server2Client{
			Key: protocol.Server2ClientKey_TopicExisted,
			Partitions: topic.getPartitions(),
		}
	} else {
		myLogger.Logger.Printf("create topic : %s %d", topicName, int(partionNum))
		topic = newTopic(topicName, int(partionNum), c.broker)
		c.broker.addTopic(&topicName, topic)
		response = &protocol.Server2Client{
			Key: protocol.Server2ClientKey_Success,
			Partitions: topic.getPartitions(),
		}
	}
	return response
}

func (c *client) getPublisherPartition(request *protocol.Client2Server)   (response *protocol.Server2Client) {
	topicName := request.Topic
	topic, ok := c.broker.getTopic(&topicName)
	if ok {
		myLogger.Logger.Printf("getPublisherPartition : %s", topicName)
		response = &protocol.Server2Client{
			Key: protocol.Server2ClientKey_Success,
			Partitions: topic.getPartitions(),
		}
	} else {
		myLogger.Logger.Printf("Partition Not existed : %s", topicName)
		response = &protocol.Server2Client{
			Key: protocol.Server2ClientKey_TopicNotExisted,
		}
	}
	return response
}
func (c *client) publish(request *protocol.Client2Server)   (response *protocol.Server2Client) {
	topicName := request.Topic
	partitionName := request.Partition
	msg := request.Msg
	topic, ok := c.broker.getTopic(&topicName)
	if !ok {
		myLogger.Logger.Printf("Topic Not existed : %s", topicName)
		response = &protocol.Server2Client{
			Key: protocol.Server2ClientKey_TopicNotExisted,
		}
		return response
	}else{
		partition, ok := topic.getPartition(&partitionName)
		if !ok {
			myLogger.Logger.Printf("Partition Not existed : %s", partitionName)
			response = &protocol.Server2Client{
				Key: protocol.Server2ClientKey_TopicNotExisted,
			}
			return response
		}else{
			myLogger.Logger.Printf("publish msg : %s", msg.String())
			partition.msgChan <- msg
			response = &protocol.Server2Client{
				Key: protocol.Server2ClientKey_Success,
			}
			return response
		}
	}

}
func (c *client) getConsumerPartition(request *protocol.Client2Server)   (response *protocol.Server2Client) {
	groupName := request.GroupName
	group, ok := c.broker.getGroup(&groupName)
	myLogger.Logger.Printf("registerComsummer : %s", groupName)
	if !ok {
		return nil
		//group = newGroup(groupName)
		//c.broker.addGroup(group)
	}
	partitions := group.getClientPartition(c)
	if partitions == nil{//不存在
		return nil
	}
	response = &protocol.Server2Client{
		Key: protocol.Server2ClientKey_ChangeConsumerPartition,
		Partitions: partitions,
	}
	return response
}
func (c *client) subscribeTopic(request *protocol.Client2Server)   (response *protocol.Server2Client) {
	topicName := request.Topic
	topic, ok := c.broker.getTopic(&topicName)
	if !ok {
		myLogger.Logger.Printf("Topic Not existed : %s", topicName)
		response = &protocol.Server2Client{
			Key: protocol.Server2ClientKey_Error,
		}
		return response
	}

	groupName := request.GroupName
	c.belongGroup = groupName
	group, ok := c.broker.getGroup(&groupName)
	myLogger.Logger.Printf("registerComsummer : %s", groupName)
	if !ok {
		myLogger.Logger.Printf("newGroup : %s", groupName)
		group = newGroup(groupName)
		c.broker.addGroup(group)
	}
	succ:= group.addTopic(topic)
	succ = group.addClient(c) || succ
	if succ{
		group.rebalance()
		return nil
	}else{
		response = &protocol.Server2Client{
			Key: protocol.Server2ClientKey_TopicExisted,
		}
		return response
	}
}


func (c *client) subscribePartition(request *protocol.Client2Server)   (response *protocol.Server2Client) {
	topicName := request.Topic
	partitionName := request.Partition
	groupName := request.GroupName
	topic, ok := c.broker.getTopic(&topicName)
	if !ok {
		myLogger.Logger.Printf("Topic Not existed : %s", topicName)
		response = &protocol.Server2Client{
			Key: protocol.Server2ClientKey_TopicNotExisted,
		}
		return response
	}else{
		partition, ok := topic.getPartition(&partitionName)
		if !ok {
			myLogger.Logger.Printf("Partition Not existed : %s", partitionName)
			response = &protocol.Server2Client{
				Key: protocol.Server2ClientKey_Error,
			}
			return response
		}else{
			c.consumePartions[partitionName] = true
			partition.addComsummerClient(c, groupName)
			response = &protocol.Server2Client{
				Key: protocol.Server2ClientKey_Success,
			}
			return response
		}
	}
}

func (c *client) registerConsumer(request *protocol.Client2Server)   (response *protocol.Server2Client) {
	groupName := request.GroupName
	group, ok := c.broker.getGroup(&groupName)
	myLogger.Logger.Printf("registerComsummer : %s", groupName)
	if !ok {
		group = newGroup(groupName)
		c.broker.addGroup(group)
	}
	succ := group.addClient(c)
	if succ{//已通过go balance response
		return nil
	}
	response = &protocol.Server2Client{
		Key: protocol.Server2ClientKey_ChangeConsumerPartition,
		Partitions: group.getClientPartition(c),
	}
	return response
}

func (c *client) unRegisterConsumer(request *protocol.Client2Server)   (response *protocol.Server2Client) {
	groupName := request.GroupName
	group, ok := c.broker.getGroup(&groupName)
	myLogger.Logger.Printf("registerComsummer : %s", groupName)
	if !ok {
		myLogger.Logger.Printf("group not exist : %s", groupName)
		return nil
	}
	group.deleteClient(c)
	return nil
}



func (c *client) sendResponse(response *protocol.Server2Client)  error{
	data, err := proto.Marshal(response)
	myLogger.Logger.Print("send sendResponse len:", len(data), response)
	if err != nil {
		myLogger.Logger.Print("marshaling error: ", err)
		return err
	}
	var buf [4]byte
	bufs := buf[:]
	binary.BigEndian.PutUint32(bufs, uint32(len(data)))
	c.writerLock.Lock()
	c.writer.Write(bufs)
	c.writer.Write(data)
	c.writer.Flush()
	c.writerLock.Unlock()
	return nil
}
