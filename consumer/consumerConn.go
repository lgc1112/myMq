package consumer

import (
	"../mylib/myLogger"
	"../mylib/protocalFuc"
	"bufio"
	"errors"
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
	writeChan     chan []byte
	exitChan chan string
}

func NewConn(addr string, consumer *Consumer)  (*consumerConn, error){
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}
	c := &consumerConn{
		addr: addr,
		conn:conn,
		reader: bufio.NewReader(conn),
		writer: bufio.NewWriter(conn),
		writeChan: make(chan []byte),
		exitChan: make(chan string),
		consumer: consumer,
	}

	return c, nil
}

//连接处理函数
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

//退出连接
func (c *consumerConn)exit()  {
	c.consumer.deleteBrokerConn(c)
	c.conn.Close()
	close(c.writeChan)
	close(c.exitChan)
}

//负责从broker读消息
func (c *consumerConn)readLoop()  {
	for{
		cmd, msgBody, err:= protocalFuc.ReadAndUnPackClientServerProtoBuf(c.reader)
		if err != nil {
			if err == io.EOF {
				myLogger.Logger.Print("EOF")
			} else {
				myLogger.Logger.Print(err)
			}
			c.exitChan <- "bye"
			break
		}else{
			myLogger.Logger.Printf("receive cmd: %s data: %s", cmd, msgBody)
		}
		c.consumer.readChan <- &readData{c.addr, cmd, msgBody}
	}
}

//负责写消息到broker
func (c *consumerConn)writeLoop()  {
	timeTicker := time.NewTicker(200 * time.Millisecond) //每200m秒触发一次
	for{
		select {
		case <- timeTicker.C:
			err := c.writer.Flush()
			if err != nil{
				myLogger.Logger.PrintError("writeLoop1:", err)
			}
		case request := <-c.writeChan:
			myLogger.Logger.Printf("writeLoop1: %s", request)
			_, err := c.writer.Write(request)
			if err != nil{
				myLogger.Logger.PrintError("writeLoop1:", err)
			}
			myLogger.Logger.Printf("writeLoop2: %s", request)
		case <- c.exitChan:
			goto exit

		}
	}
	exit:
		timeTicker.Stop()
		myLogger.Logger.Printf("writeLoop exit:")
}

//往连接中放数据
func (c *consumerConn) Put(data []byte) error{

	select {
	case c.writeChan <- data:
		//myLogger.Logger.Print("do not have client")
	case <-time.After(2000 * time.Microsecond):
		myLogger.Logger.PrintError("write fail")
		return errors.New("write fail")
	}

	return nil

}

