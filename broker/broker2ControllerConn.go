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
	conn net.Conn
	reader *bufio.Reader
	writer *bufio.Writer

	broker *Broker

	addr string //连接的地址
	writeMsgChan     chan *protocol.Broker2Controller
	exitChan chan string
}

//func NewBroker2ControllerConn(conn net.Conn, broker *Broker)  *broker2ControllerConn{
//	c := &broker2ControllerConn{
//		conn: conn,
//		reader: bufio.NewReader(conn),
//		writer: bufio.NewWriter(conn),
//		broker: broker,
//		writeMsgChan: make(chan []byte),
//		exitChan: make(chan string),
//	}
//	return c
//}


func NewBroker2ControllerConn(addr string, broker *Broker)  (*broker2ControllerConn, error){
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}
	c := &broker2ControllerConn{
		addr:addr,
		conn: conn,
		reader: bufio.NewReader(conn),
		writer: bufio.NewWriter(conn),
		broker: broker,
		writeMsgChan: make( chan *protocol.Broker2Controller),
		exitChan: make(chan string),
	}
	go c.clientHandle()
	return c, nil
}

func (b *broker2ControllerConn)clientHandle() {
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

func (b *broker2ControllerConn)Put(broker2ControllerData *protocol.Broker2Controller) error{
	select {
	case b.writeMsgChan <- broker2ControllerData:
		//myLogger.Logger.Print("do not have client")
	case <-time.After(100 * time.Microsecond):
		myLogger.Logger.PrintWarning("broker2ControllerData Put fail")
		return errors.New("put fail")
	}
	return nil
}

func (b *broker2ControllerConn)clientExit() {
	close(b.writeMsgChan)
	close(b.exitChan)
}

func (b *broker2ControllerConn)Close() {
	b.conn.Close()
}
func (b *broker2ControllerConn) writeLoop() {
	for{
		select {
		case s := <- b.exitChan:
			myLogger.Logger.Print(s)
			goto exit
		case broker2ControllerData := <- b.writeMsgChan: //如果向客户端发送了一条消息，则客户端目前可接受的数据量readyCount应该减一
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
	//myLogger.Logger.Print("close writeLoop")
	return
}

func (b *broker2ControllerConn)Write(data []byte) (error){

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
func (b *broker2ControllerConn)readLoop() {
	for{
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
		}else{
			myLogger.Logger.Printf("receive broker2ControllerConn: %s", data)
		}

		switch controller2BrokerData.Key {
		case protocol.Controller2BrokerKey_DeletePartition:

		case protocol.Controller2BrokerKey_CreadtPartition:

		default:
			myLogger.Logger.Print("cannot find key")
		}

	}
}
