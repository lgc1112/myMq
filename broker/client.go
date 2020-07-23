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
const writeChanSize int = 0
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
	//broker.clientChangeChan <- &clientChange{true, c}
	waitFinished := make(chan bool)
	c.broker.clientChangeChan <- &clientChange{true, c, waitFinished}//放到broker 的readLoop协程中进行处理，避免频繁使用锁
	<- waitFinished //等待添加
	//broker.addClient(c)
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
	waitFinished := make(chan bool)
	c.broker.clientChangeChan <- &clientChange{false, c, waitFinished}//放到broker 的readLoop协程中进行处理，避免频繁使用锁
	<- waitFinished //等待移除
	//c.broker.removeClient(c.id)
	//从group中删除
	group, ok := c.broker.getGroup(&c.belongGroup)
	if !ok {
		myLogger.Logger.Print("exit client do not belong to any group")
	}else{
		myLogger.Logger.Printf("exit client belong to group : %s", c.belongGroup)
		succ:= group.deleteClient(c.id) //从group中删除
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
			data, err := proto.Marshal(response)
			//myLogger.Logger.Print("send sendResponse len:", len(data), response)
			if err != nil {
				myLogger.Logger.Print("marshaling error: ", err)
				continue
			}
			var buf [4]byte
			bufs := buf[:]
			binary.BigEndian.PutUint32(bufs, uint32(len(data)))
			c.writerLock.Lock()
			c.writer.Write(bufs)
			c.writer.Write(data)
			c.writer.Flush()
			c.writerLock.Unlock()
			//c.sendResponse(response)
		}
	}
exit:
	//myLogger.Logger.Print("close writeLoop")
	return
}
func (c *client)readLoop() {
	for{
		myLogger.Logger.Print("readLoop")
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
		client2ServerData := &protocol.Client2Server{}
		err = proto.Unmarshal(requestData, client2ServerData)
		if err != nil {
			myLogger.Logger.Print("Unmarshal error %s", err)
		}else{
			myLogger.Logger.Printf("receive client2ServerData: %s", client2ServerData)
		}

		c.broker.readChan <- &readData{c.id, client2ServerData}

		//var response *protocol.Server2Client
		//switch request.Key {
		//case protocol.Client2ServerKey_CreatTopic:
		//	response = c.creatTopic(request)
		//case protocol.Client2ServerKey_GetPublisherPartition:
		//	response = c.getPublisherPartition(request)
		//case protocol.Client2ServerKey_Publish:
		//	response = c.publish(request)
		//case protocol.Client2ServerKey_GetConsumerPartition:
		//	response = c.getConsumerPartition(request)
		//case protocol.Client2ServerKey_SubscribePartion:
		//	response = c.subscribePartition(request)
		//case protocol.Client2ServerKey_SubscribeTopic:
		//	response = c.subscribeTopic(request)
		//case protocol.Client2ServerKey_RegisterConsumer:
		//	response = c.registerConsumer(request)
		//case protocol.Client2ServerKey_UnRegisterConsumer:
		//	response = c.unRegisterConsumer(request)
		//default:
		//	myLogger.Logger.Print("cannot find key");
		//}
		//if response != nil{
		//	c.sendResponse(response)
		//}
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
