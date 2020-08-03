package consumer

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

type consumerConn struct {
	addr string
	consumer *Consumer
	conn net.Conn
	reader *bufio.Reader
	//writerLock sync.RWMutex
	writer *bufio.Writer
	writeChan     chan *protocol.Client2Server
	exitChan chan string
}

func newConn(addr string, consumer *Consumer)  (*consumerConn, error){
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}
	c := &consumerConn{
		addr: addr,
		conn:conn,
		reader: bufio.NewReader(conn),
		writer: bufio.NewWriter(conn),
		writeChan: make(chan *protocol.Client2Server),
		exitChan: make(chan string),
		consumer: consumer,
	}

	return c, nil
}
func (c *consumerConn)Handle() {
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
	myLogger.Logger.Print("connect end")
	c.exit()
}
func (c *consumerConn)exit()  {
	c.consumer.removeBrokerConn(c)
	c.conn.Close()
	close(c.writeChan)
	close(c.exitChan)
}

func (c *consumerConn)readLoop()  {
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
		server2ClientData := &protocol.Server2Client{}
		err = proto.Unmarshal(requestData, server2ClientData)
		if err != nil {
			myLogger.Logger.Print("Unmarshal error %s", err)
		}else{
			myLogger.Logger.Printf("receive data: %s", server2ClientData)
		}
		c.consumer.readChan <- &readData{c.addr, server2ClientData}
	}
}

func (c *consumerConn)writeLoop()  {
	var request *protocol.Client2Server
	for{
		select {
		case request = <-c.writeChan:
			data, err := proto.Marshal(request)
			if err != nil {
				myLogger.Logger.Print("marshaling error: ", err)
				continue
			}
			var buf [4]byte
			bufs := buf[:]
			binary.BigEndian.PutUint32(bufs, uint32(len(data)))
			//c.writerLock.Lock()
			c.writer.Write(bufs)
			c.writer.Write(data)
			c.writer.Flush()
			//c.writerLock.Unlock()
			myLogger.Logger.Printf("write: %s", request)
		case <- c.exitChan:
			goto exit

		}
	}
	exit:
		myLogger.Logger.Printf("writeLoop exit:")
}
func (c *consumerConn) Put(data *protocol.Client2Server) error{

	select {
	case c.writeChan <- data:
		//myLogger.Logger.Print("do not have client")
	case <-time.After(100 * time.Microsecond):
		myLogger.Logger.PrintError("write controller2BrokerConn fail")
		return errors.New("write controller2BrokerConn fail")
	}

	return nil

}