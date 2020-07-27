package producer

import (
	"../mylib/myLogger"
	"../protocol"
	"bufio"
	"encoding/binary"
	"github.com/golang/protobuf/proto"
	"io"
	"net"
)

type producerConn struct {
	addr string
	producer *Producer
	reader *bufio.Reader
	writer *bufio.Writer
	conn net.Conn
	writeChan     chan *protocol.Client2Server
	exitChan chan string
}

func newConn(addr string, producer *Producer)  (*producerConn, error){
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}
	c := &producerConn{
		addr: addr,
		reader: bufio.NewReader(conn),
		writer: bufio.NewWriter(conn),
		producer: producer,
		conn: conn,
	}
	return c, nil
}


func (p *producerConn)readResponse() (*protocol.Server2Client, error){
	//myLogger.Logger.Print("reading")
	tmp := make([]byte, 4)
	_, err := io.ReadFull(p.reader, tmp) //读取长度
	if err != nil {
		if err == io.EOF {
			myLogger.Logger.Print("EOF")
		} else {
			myLogger.Logger.Print(err)
		}
		return nil, err
	}
	len := int32(binary.BigEndian.Uint32(tmp))
	myLogger.Logger.Printf("readLen %d ", len)
	requestData := make([]byte, len)
	_, err = io.ReadFull(p.reader, requestData) //读取内容
	if err != nil {
		if err == io.EOF {
			myLogger.Logger.Print("EOF")
		} else {
			myLogger.Logger.Print(err)
		}
		return nil, err
	}
	response := &protocol.Server2Client{}
	err = proto.Unmarshal(requestData, response)
	if err != nil {
		myLogger.Logger.Print("Unmarshal error %s", err)
		return nil, err
	}
	myLogger.Logger.Printf("receive response Key:%s : %s", response.Key, response)
	return response, nil

}
//func (p *producerConn)Handle() {
//	var wg sync.WaitGroup
//	wg.Add(2)
//	go func() {
//		//p.readLoop()
//		wg.Done()
//	}()
//	go func() {
//		p.writeLoop()
//		wg.Done()
//	}()
//	wg.Wait()
//	myLogger.Logger.Print("connect end")
//	p.exit()
//}
//func (p *producerConn)exit()  {
//	p.producer.removeBrokerConn(p)
//	p.conn.Close()
//	close(p.writeChan)
//	close(p.exitChan)
//}
//func (p *producerConn)readLoop()  {
//	for{
//		myLogger.Logger.Print("readLoop")
//		tmp := make([]byte, 4)
//		_, err := io.ReadFull(p.reader, tmp) //读取长度
//		if err != nil {
//			if err == io.EOF {
//				myLogger.Logger.Print("EOF")
//			} else {
//				myLogger.Logger.Print(err)
//			}
//			p.exitChan <- "bye"
//			break
//		}
//		len := int32(binary.BigEndian.Uint32(tmp))
//		myLogger.Logger.Printf("readLen %d ", len)
//		requestData := make([]byte, len)
//		_, err = io.ReadFull(p.reader, requestData) //读取内容
//		if err != nil {
//			if err == io.EOF {
//				myLogger.Logger.Print("EOF")
//			} else {
//				myLogger.Logger.Print(err)
//			}
//			p.exitChan <- "bye"
//			break
//		}
//		server2ClientData := &protocol.Server2Client{}
//		err = proto.Unmarshal(requestData, server2ClientData)
//		if err != nil {
//			myLogger.Logger.Print("Unmarshal error %s", err)
//		}else{
//			myLogger.Logger.Printf("receive data: %s", server2ClientData)
//		}
//		p.producer.readChan <- &readData{p.addr, server2ClientData}
//	}
//}
//func (p *producerConn)writeLoop()  {
//	var request *protocol.Client2Server
//	for{
//		select {
//		case request = <-p.writeChan:
//			data, err := proto.Marshal(request)
//			if err != nil {
//				myLogger.Logger.Print("marshaling error: ", err)
//				continue
//			}
//			var buf [4]byte
//			bufs := buf[:]
//			binary.BigEndian.PutUint32(bufs, uint32(len(data)))
//			//p.writerLock.Lock()
//			p.writer.Write(bufs)
//			p.writer.Write(data)
//			p.writer.Flush()
//			//p.writerLock.Unlock()
//			myLogger.Logger.Printf("write: %s", request)
//		case <- p.exitChan:
//			goto exit
//
//		}
//	}
//exit:
//	myLogger.Logger.Printf("writeLoop exit:")
//}
