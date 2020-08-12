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
	id     int64
	conn   net.Conn
	reader *bufio.Reader
	writer *bufio.Writer

	broker           *Broker
	clientListenAddr string //客户端的监听地址

	writeMsgChan chan *protocol.Controller2Broker
	exitChan     chan string
}

//controller到broker的连接
func NewController2BrokerConn(conn net.Conn, broker *Broker) *controller2BrokerConn {
	c := &controller2BrokerConn{
		id:           broker.GenerateClientId(),
		conn:         conn,
		reader:       bufio.NewReader(conn),
		writer:       bufio.NewWriter(conn),
		broker:       broker,
		writeMsgChan: make(chan *protocol.Controller2Broker),
		exitChan:     make(chan string),
	}
	return c
}

//连接处理函数
func (c *controller2BrokerConn) clientHandle() {
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
	//c.broker.etcdClient.PutControllerAddr(c.broker.maddr)
}

//连接退出
func (c *controller2BrokerConn) clientExit() {
	c.broker.DeleteBrokerConn(c) //删除
	close(c.writeMsgChan)
	close(c.exitChan)
}

func (c *controller2BrokerConn) Write(data []byte) error {
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

//写数据
func (c *controller2BrokerConn) Put(data *protocol.Controller2Broker) error {
	select {
	case c.writeMsgChan <- data:
		//myLogger.Logger.Print("do not have client")
	case <-time.After(5000 * time.Microsecond):
		myLogger.Logger.PrintError("write controller2BrokerConn fail")
		return errors.New("write controller2BrokerConn fail")
	}

	return nil
}

//写数据到broker
func (c *controller2BrokerConn) writeLoop() {
	for {
		select {
		case s := <-c.exitChan:
			myLogger.Logger.Print(s)
			goto exit
		case controller2BrokerData := <-c.writeMsgChan:
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
	if !c.broker.needExit {
		c.broker.GetAndDeletePartitionForBroker(&c.clientListenAddr) //该broker挂了，删除对应分区
	}
	//myLogger.Logger.Print("close writeLoop")
	return
}

//读取broker发来的数据
func (c *controller2BrokerConn) readLoop() {
	for {
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
		} else {
			myLogger.Logger.Print("receive broker2ControllerData:", broker2ControllerData)
		}

		switch broker2ControllerData.Key {
		case protocol.Broker2ControllerKey_RegisterBroker:
			c.clientListenAddr = broker2ControllerData.Addr.ClientListenAddr
			c.broker.AddBrokerConn(c) //注册
			partitions, dirtyTopics := c.broker.GetAndAddPartitionForBroker(&broker2ControllerData.Addr.ClientListenAddr)
			for _, partition := range partitions {
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
			myLogger.Logger.PrintfDebug2("新broker注册")
			if openCluster {
				for topicName, _ := range *dirtyTopics { //上传到etcd节点
					topic, _ := c.broker.getTopic(&topicName)
					pars := &protocol.Partitions{ //创建分区的消息
						Partition: topic.getPartitions(),
					}
					data, err := proto.Marshal(pars)
					if err != nil {
						myLogger.Logger.PrintError("marshaling error: ", err)
						continue
					}

					myLogger.Logger.Print(topicName, " Put Patitions : ", pars)
					c.broker.etcdClient.PutPatitions(topicName, string(data)) //将分区改变保存到集群
				}
			}
		case protocol.Broker2ControllerKey_Heartbeat:
			myLogger.Logger.Print("Broker2ControllerKey_Heartbeat")
		case protocol.Broker2ControllerKey_CreadtPartitionSuccess:
			myLogger.Logger.Print("CreadtPartitionSuccess")
		default:
			myLogger.Logger.Print("cannot find key")
		}
	}
}
