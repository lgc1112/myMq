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

type broker2ControllerConn struct {
	conn   net.Conn
	reader *bufio.Reader
	writer *bufio.Writer

	broker       *Broker
	addr         string //连接的地址
	writeMsgChan chan *protocol.Broker2Controller
	exitChan     chan string
}

//处理普通broker到controller的连接
func NewBroker2ControllerConn(addr string, broker *Broker) (*broker2ControllerConn, error) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}
	c := &broker2ControllerConn{
		addr:         addr,
		conn:         conn,
		reader:       bufio.NewReader(conn),
		writer:       bufio.NewWriter(conn),
		broker:       broker,
		writeMsgChan: make(chan *protocol.Broker2Controller),
		exitChan:     make(chan string),
	}
	go c.clientHandle()
	return c, nil
}

//连接处理函数
func (b *broker2ControllerConn) clientHandle() {
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		b.readLoop()
		wg.Done()
	}()
	go func() {
		b.writeLoop()
		wg.Done()
	}()
	wg.Wait()
	b.clientExit()
	b.broker.broker2ControllerConn = nil
	myLogger.Logger.Print("a broker2ControllerConn leave")
}

//添加数据
func (b *broker2ControllerConn) Put(broker2ControllerData *protocol.Broker2Controller) error {
	select {
	case b.writeMsgChan <- broker2ControllerData:
		//myLogger.Logger.Print("do not have client")
	case <-time.After(500 * time.Microsecond):
		myLogger.Logger.PrintWarning("broker2ControllerData Put fail")
		return errors.New("put fail")
	}
	return nil
}

//退出client
func (b *broker2ControllerConn) clientExit() {
	close(b.writeMsgChan)
	close(b.exitChan)
}

//关闭连接
func (b *broker2ControllerConn) Close() {
	b.conn.Close()
}

//写数据协程
func (b *broker2ControllerConn) writeLoop() {
	for {
		select {
		case s := <-b.exitChan:
			myLogger.Logger.Print(s)
			goto exit
		case broker2ControllerData := <-b.writeMsgChan: //向客户端发送了一条消息
			myLogger.Logger.Printf("broker2ControllerConn write %s", broker2ControllerData.String())
			data, err := proto.Marshal(broker2ControllerData)
			//myLogger.Logger.Print("send sendResponse len:", len(data), response)
			if err != nil {
				myLogger.Logger.PrintError("marshaling error: ", err)
				continue
			}
			err = b.Write(data)
			if err != nil {
				myLogger.Logger.PrintError("marshaling error: ", err)
				continue
			}
		}
	}
exit:
	myLogger.Logger.Print("close writeLoop")
	return
}

//写数据协程
func (b *broker2ControllerConn) Write(data []byte) error {
	var buf [4]byte
	bufs := buf[:]
	binary.BigEndian.PutUint32(bufs, uint32(len(data)))
	_, err := b.writer.Write(bufs)
	if err != nil {
		myLogger.Logger.PrintError("writer error: ", err)
		return err
	}
	_, err = b.writer.Write(data)
	if err != nil {
		myLogger.Logger.PrintError("writer error: ", err)
		return err
	}
	err = b.writer.Flush()
	if err != nil {
		myLogger.Logger.PrintError("writer error: ", err)
		return err
	}
	return nil
}

//读数据协程
func (b *broker2ControllerConn) readLoop() {
	for {
		myLogger.Logger.Print("readLoop")
		tmp := make([]byte, 4)
		_, err := io.ReadFull(b.reader, tmp) //读取长度
		if err != nil {
			if err == io.EOF {
				myLogger.Logger.Print("EOF")
			} else {
				myLogger.Logger.Print(err)
			}
			b.exitChan <- "bye"
			break
		}
		len := int32(binary.BigEndian.Uint32(tmp))
		myLogger.Logger.Printf("readLen %d ", len)
		data := make([]byte, len)
		_, err = io.ReadFull(b.reader, data) //读取内容

		controller2BrokerData := &protocol.Controller2Broker{}
		err = proto.Unmarshal(data, controller2BrokerData)
		if err != nil {
			myLogger.Logger.PrintError("Unmarshal error %s", err)
		} else {
			myLogger.Logger.Printf("receive broker2ControllerConn: %s", data)
		}

		switch controller2BrokerData.Key {
		case protocol.Controller2BrokerKey_DeletePartition:
			if controller2BrokerData.Partitions.Addr != b.broker.maddr.ClientListenAddr {
				myLogger.Logger.PrintError("should not come here")
				break
			}
			b.broker.partitionMapLock.Lock()
			partition, ok := b.broker.getPartition(&controller2BrokerData.Partitions.Name)
			if !ok {
				myLogger.Logger.PrintWarning("try to delete not exist partition:", controller2BrokerData.Partitions.Name)
			} else {
				myLogger.Logger.Printf("Delete Partition : %s", controller2BrokerData.Partitions.Name)
				b.broker.deletePartition(&controller2BrokerData.Partitions.Name)
				partition.exit()
			}
			b.broker.partitionMapLock.Unlock()
		case protocol.Controller2BrokerKey_CreadtPartition:
			if controller2BrokerData.Partitions.Addr != b.broker.maddr.ClientListenAddr {
				myLogger.Logger.PrintError("should not come here")
				break
			}
			partition := newPartition(controller2BrokerData.Partitions.Name, controller2BrokerData.Partitions.Addr, true, b.broker, "") //创建分区
			myLogger.Logger.Printf("Create Partition : %s", controller2BrokerData.Partitions.Name)
			b.broker.addPartition(&controller2BrokerData.Partitions.Name, partition) //保存分区
			response := &protocol.Broker2Controller{
				Key: protocol.Broker2ControllerKey_CreadtPartitionSuccess,
			}
			b.writeMsgChan <- response
		default:
			myLogger.Logger.Print("cannot find key")
		}

	}
}
