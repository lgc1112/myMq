package broker

import (
	"../mylib/myLogger"
	"../protocol"
	"bufio"
	"encoding/binary"
	"errors"
	"github.com/golang/protobuf/proto"
	"io"
	"net"
	"sync"
	"time"
)
type controller2BrokerConn struct {
	id int64
	conn net.Conn
	reader *bufio.Reader
	writer *bufio.Writer

	broker *Broker
	clientListenAddr string//客户端的监听地址

	writeMsgChan     chan *protocol.Controller2Broker
	exitChan chan string
}

func NewController2BrokerConn(conn net.Conn, broker *Broker)  *controller2BrokerConn{
	c := &controller2BrokerConn{
		id : broker.GenerateClientId(),
		conn: conn,
		reader: bufio.NewReader(conn),
		writer: bufio.NewWriter(conn),
		broker: broker,
		writeMsgChan: make(chan *protocol.Controller2Broker),
		exitChan: make(chan string),
	}
	return c
}


func (c *controller2BrokerConn)clientHandle() {
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
	c.clientExit()
	myLogger.Logger.Print("a controller2BrokerConn leave")
	c.broker.etcdClient.PutControllerAddr(c.broker.maddr)
}

func (c *controller2BrokerConn)clientExit() {
	c.broker.DeleteBrokerConn(c) //删除
	close(c.writeMsgChan)
	close(c.exitChan)
}

func (c *controller2BrokerConn)Write(data []byte) (error){

	var buf [4]byte
	bufs := buf[:]
	binary.BigEndian.PutUint32(bufs, uint32(len(data)))
	_, err := c.writer.Write(bufs)
	if err != nil {
		myLogger.Logger.PrintError("writer error: ", err)
		return err
	}
	_, err = c.writer.Write(data)
	if err != nil {
		myLogger.Logger.PrintError("writer error: ", err)
		return err
	}
	err = c.writer.Flush()
	if err != nil {
		myLogger.Logger.PrintError("writer error: ", err)
		return err
	}
	return nil
}

func (c *controller2BrokerConn)Put(data *protocol.Controller2Broker) (error){
	select {
	case c.writeMsgChan <- data:
		//myLogger.Logger.Print("do not have client")
	case <-time.After(500 * time.Microsecond):
		myLogger.Logger.PrintError("write controller2BrokerConn fail")
		return errors.New("write controller2BrokerConn fail")
	}

	return nil
}

func (c *controller2BrokerConn) writeLoop() {
	for{
		select {
		case s := <- c.exitChan:
			myLogger.Logger.Print(s)
			goto exit
		case controller2BrokerData := <- c.writeMsgChan:
			myLogger.Logger.Printf("broker2ControllerConn write %s", controller2BrokerData)
			data, err := proto.Marshal(controller2BrokerData)
			//myLogger.Logger.Print("send sendResponse len:", len(data), response)
			if err != nil {
				myLogger.Logger.PrintError("marshaling error: ", err)
				continue
			}
			err = c.Write(data)
			if err != nil {
				myLogger.Logger.PrintError("marshaling error: ", err)
				continue
			}
		}
	}
exit:
	//myLogger.Logger.Print("close writeLoop")
	return
}
func (c *controller2BrokerConn)readLoop() {
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
		data := make([]byte, len)
		_, err = io.ReadFull(c.reader, data) //读取内容

		broker2ControllerData := &protocol.Broker2Controller{}
		err = proto.Unmarshal(data, broker2ControllerData)
		if err != nil {
			myLogger.Logger.PrintError("Unmarshal error %s", err)
		}else{
			myLogger.Logger.Print("receive broker2ControllerData:", broker2ControllerData)
		}

		switch broker2ControllerData.Key {
		case protocol.Broker2ControllerKey_RegisterBroker:
			c.clientListenAddr = broker2ControllerData.Addr.ClientListenAddr
			c.broker.AddBrokerConn(c) //注册
			partitions := c.broker.getPartitionForBroker(&broker2ControllerData.Addr.ClientListenAddr)
			for _, partition := range partitions{
				controller2BrokerData := &protocol.Controller2Broker{ //创建分区的消息
					Key: protocol.Controller2BrokerKey_CreadtPartition,
					Partitions: &protocol.Partition{
						Name: partition.Name,
						Addr: partition.Addr,
					},
				}
				err = c.Put(controller2BrokerData) //发送
				if err != nil {
					myLogger.Logger.PrintError("CreadtPartition error:", err, controller2BrokerData)
				}
			}
			//c.broker.RebanlenceAllGroup()
			c.broker.etcdClient.PutControllerAddr(c.broker.maddr)

		case protocol.Broker2ControllerKey_Heartbeat:

		case protocol.Broker2ControllerKey_CreadtPartitionSuccess:
			myLogger.Logger.Print("CreadtPartitionSuccess")
		default:
			myLogger.Logger.Print("cannot find key")
		}
	}
}
