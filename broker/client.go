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
type client struct {
	id int64
	Reader *bufio.Reader
	Writer *bufio.Writer
	broker *Broker
	writeChan     chan *protocol.Response
}

func newClient(conn net.Conn, broker *Broker)  *client{
	c := &client{
		id : atomic.AddInt64(&broker.maxClientId, 1),
		Reader: bufio.NewReader(conn),
		Writer: bufio.NewWriter(conn),
		broker: broker,
		writeChan: make(chan *protocol.Response, writeChanSize),
	}
	broker.addClient(c)
	return c;
}

func (c *client)clientHandle(conn net.Conn) {
	var wg sync.WaitGroup
	wg.Add(2);
	go func() {
		c.readLoop();
		wg.Done()
	}()
	go func() {
		c.writeLoop();
		wg.Done()
	}()
	wg.Wait();
	c.broker.loger.Print("a client leave");
	conn.Close()
	c.broker.removeClient(c.id)
}
func (c *client) writeLoop() {
	var response *protocol.Response
	for{
		select {
		case response = <- c.writeChan:
			myLogger.Logger.Printf("writeResponse %s", response.String())
			c.sendResponse(response)
		}
	}
	return
}
func (c *client)readLoop() {
	for{
		c.broker.loger.Print("readLoop");
		tmp := make([]byte, 4)
		_, err := io.ReadFull(c.Reader, tmp) //读取长度
		if err != nil {
			if err == io.EOF {
				c.broker.loger.Print("EOF")
			} else {
				c.broker.loger.Print(err)
			}
			break
		}
		len := int32(binary.BigEndian.Uint32(tmp))
		c.broker.loger.Printf("readLen %d ", len);
		requestData := make([]byte, len)
		_, err = io.ReadFull(c.Reader, requestData) //读取内容
		if err != nil {
			if err == io.EOF {
				c.broker.loger.Print("EOF")
			} else {
				c.broker.loger.Print(err)
			}
			break
		}
		request := &protocol.Request{}
		err = proto.Unmarshal(requestData, request)
		if err != nil {
			c.broker.loger.Print("Unmarshal error %s", err);
		}else{
			c.broker.loger.Printf("receive request Key:%s : %s", request.Key, request);
		}
		var response *protocol.Response
		switch request.Key {
		case protocol.RequestKey_CreatTopic:
			response = c.creatTopic(request)
		case protocol.RequestKey_GetPublisherPartition:
			response = c.getPublisherPartition(request)
		case protocol.RequestKey_Publish:
			response = c.publish(request)
		case protocol.RequestKey_GetConsumerPartition:
			response = c.getConsumerPartition(request)
		case protocol.RequestKey_Subscribe:
			response = c.subscribe(request)
		default:
			c.broker.loger.Print("cannot find key");
		}
		if response != nil{
			c.sendResponse(response)
		}
	}
}

func (c *client) creatTopic(request *protocol.Request)  (response *protocol.Response) {
	topicName := request.Topic
	partionNum := request.PartitionNum
	topic, ok := c.broker.getTopic(&topicName)
	if ok {
		c.broker.loger.Printf("try to create existed topic : %s %d", topicName, int(partionNum))
		response = &protocol.Response{
			ResponseType: protocol.ResponseType_TopicExisted,
			Partitions: topic.getPartitions(),
		}
	} else {
		c.broker.loger.Printf("create topic : %s %d", topicName, int(partionNum))
		topic = newTopic(topicName, int(partionNum), c.broker)
		c.broker.addTopic(&topicName, topic)
		response = &protocol.Response{
			ResponseType: protocol.ResponseType_Success,
			Partitions: topic.getPartitions(),
		}
	}
	return response
}

func (c *client) getPublisherPartition(request *protocol.Request)   (response *protocol.Response) {
	topicName := request.Topic
	topic, ok := c.broker.getTopic(&topicName)
	if ok {
		c.broker.loger.Printf("getPublisherPartition : %s", topicName)
		response = &protocol.Response{
			ResponseType: protocol.ResponseType_Success,
			Partitions: topic.getPartitions(),
		}
	} else {
		c.broker.loger.Printf("Partition Not existed : %s", topicName)
		response = &protocol.Response{
			ResponseType: protocol.ResponseType_TopicNotExisted,
		}
	}
	return response
}
func (c *client) publish(request *protocol.Request)   (response *protocol.Response) {
	topicName := request.Topic
	partitionName := request.Partition
	msg := request.Msg
	topic, ok := c.broker.getTopic(&topicName)
	if !ok {
		c.broker.loger.Printf("Topic Not existed : %s", topicName)
		response = &protocol.Response{
			ResponseType: protocol.ResponseType_TopicNotExisted,
		}
		return response
	}else{
		partition, ok := topic.getPartition(&partitionName)
		if !ok {
			c.broker.loger.Printf("Partition Not existed : %s", partitionName)
			response = &protocol.Response{
				ResponseType: protocol.ResponseType_TopicNotExisted,
			}
			return response
		}else{
			c.broker.loger.Printf("publish msg : %s", msg.String())
			partition.msgChan <- msg
			response = &protocol.Response{
				ResponseType: protocol.ResponseType_Success,
			}
			return response
		}
	}

}

func (c *client) getConsumerPartition(request *protocol.Request)   (response *protocol.Response) {
	//groupName := request.GroupName
	//group, ok := c.broker.getGroup(&groupName)
	//if ok {
	//	c.broker.loger.Printf("getConsumerPartition : %s", groupName)
	//	response = &protocol.Response{
	//		ResponseType: protocol.ResponseType_TopicExisted,
	//		Partitions: group.clients
	//	}
	//} else {
	//	c.broker.loger.Printf("create topic : %s %d", topicName, int(partionNum))
	//	topic = newTopic(topicName, int(partionNum), c.broker)
	//	c.broker.Lock()
	//	c.broker.topicMap[topicName] = topic
	//	c.broker.Unlock()
	//	response = &protocol.Response{
	//		ResponseType: protocol.ResponseType_Success,
	//		Partitions: topic.getPartitions(),
	//	}
	//}
	return nil
}
func (c *client) subscribe(request *protocol.Request)   (response *protocol.Response) {
	topicName := request.Topic
	partitionName := request.Partition
	topic, ok := c.broker.getTopic(&topicName)
	if !ok {
		c.broker.loger.Printf("Topic Not existed : %s", topicName)
		response = &protocol.Response{
			ResponseType: protocol.ResponseType_TopicNotExisted,
		}
		return response
	}else{
		partition, ok := topic.getPartition(&partitionName)
		if !ok {
			c.broker.loger.Printf("Partition Not existed : %s", partitionName)
			response = &protocol.Response{
				ResponseType: protocol.ResponseType_TopicNotExisted,
			}
			return response
		}else{
			partition.addComsummerClient(c)
			response = &protocol.Response{
				ResponseType: protocol.ResponseType_Success,
			}
			return response
		}
	}
}


func (c *client) sendResponse(response *protocol.Response)  error{
	data, err := proto.Marshal(response)
	if err != nil {
		c.broker.loger.Print("marshaling error: ", err)
		return err
	}
	var buf [4]byte
	bufs := buf[:]
	binary.BigEndian.PutUint32(bufs, uint32(len(data)))
	c.Writer.Write(bufs)
	c.Writer.Write(data)
	c.Writer.Flush()
	return nil
}
