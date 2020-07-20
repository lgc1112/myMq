package clientConn


import (
	"bufio"
	"encoding/binary"
	"io"
	"net"
	"../../protocol"
	"github.com/golang/protobuf/proto"
	"../myLogger"
	"sync"
)

type ClientConn struct {
	addr string
	writerLock sync.RWMutex
	reader *bufio.Reader
	writer *bufio.Writer
	readChan     chan *protocol.Server2Client
	writeChan     chan *protocol.Client2Server
}

func NewConn(addr string)  (*ClientConn, error){
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		myLogger.Logger.Printf("newConn error: %s", err)
		return nil, err
	}
	c := &ClientConn{
		addr: addr,
		reader: bufio.NewReader(conn),
		writer: bufio.NewWriter(conn),
		readChan: make(chan *protocol.Server2Client),
		writeChan: make(chan *protocol.Client2Server),
	}
	go c.readLoop()
	go c.writeLoop()
	return c, nil
}
func (c *ClientConn) WriteAndFlush(data []byte ){
	c.writerLock.Lock()
	c.writer.Write(data)
	c.writer.Flush()
	c.writerLock.Unlock()
}
func (c *ClientConn)readLoop(){
	var serverData *protocol.Server2Client
	var err error
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
			break
		}
		len := int32(binary.BigEndian.Uint32(tmp))
		myLogger.Logger.Printf("readLen %d ", len);
		requestData := make([]byte, len)
		_, err = io.ReadFull(c.reader, requestData) //读取内容
		if err != nil {
			if err == io.EOF {
				myLogger.Logger.Print("EOF")
			} else {
				myLogger.Logger.Print(err)
			}
			break
		}
		request := &protocol.Client2Server{}
		err = proto.Unmarshal(requestData, request)
		if err != nil {
			myLogger.Logger.Print("Unmarshal error %s", err);
		}else{
			myLogger.Logger.Printf("receive request Key:%s : %s", request.Key, request);
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
func (p *ClientConn)writeLoop(){
	var request *protocol.Client2Server
	for{
		select {
		case request = <-p.writeChan:
			data, err := proto.Marshal(request)
			if err != nil {
				myLogger.Logger.Print("marshaling error: ", err)
				continue
			}
			var buf [4]byte
			bufs := buf[:]
			binary.BigEndian.PutUint32(bufs, uint32(len(data)))
			p.writer.Write(bufs)
			p.writer.Write(data)
			p.writer.Flush()
			myLogger.Logger.Printf("write %s", request)
		}
	}
}
func (p *ClientConn)ReadResponse() (*protocol.Server2Client, error){

	myLogger.Logger.Print("readResponse")
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
	myLogger.Logger.Printf("readLen %d ", len);
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
		myLogger.Logger.Print("Unmarshal error %s", err);
		return nil, err
	}
	myLogger.Logger.Printf("receive request Key:%s : %s", response.Key, response);
	return response, nil

}
