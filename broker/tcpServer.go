package broker

import (
	"../protocol"
	"encoding/binary"
	"errors"
	"github.com/golang/protobuf/proto"
	"io"
	"log"
	"net"
	"sync"
)

type tcpServer struct {
	broker *Broker;
	addr string
}


const (
	creatTopic int32 = 0
	getPublisherPartition = 1
	publish int32 = 2
	getConsumerPartition int32 = 3
	subscribe int32 = 4
)

func newTcpServer(broker *Broker, addr string) *tcpServer{

	return &tcpServer{broker:broker, addr:addr};
}

func (t *tcpServer)startTcpServer() {
	listener, err := net.Listen("tcp", t.addr)
	t.broker.loger.Print("startTcpServer  " + t.addr);
	if err != nil {
		t.broker.loger.Print(err);
		log.Fatal(err)
	}
	for {
		conn, err := listener.Accept()
		if err != nil {
			t.broker.loger.Print(err);
			continue
		}
		t.broker.loger.Print("new client " + conn.RemoteAddr().String());
		go t.handleConn(conn) // handle one connection at a time
	}
}

func (t *tcpServer)handleConn(conn net.Conn) {
	var wg sync.WaitGroup
	client := newClient(conn);
	wg.Add(2);
	go func() {
		t.readLoop(client);
		wg.Done()
	}()
	go func() {
		t.writeLoop(client);
		wg.Done()
	}()
	wg.Wait();
	t.broker.loger.Print("a client leave");
	conn.Close()
}

func (t *tcpServer)readLoop(client *client) {
	for{
		t.broker.loger.Print("readLoop");
		tmp := make([]byte, 4)
		_, err := io.ReadFull(client.Reader, tmp) //读取长度
		if err != nil {
			if err == io.EOF {
				t.broker.loger.Print("EOF")
			} else {
				t.broker.loger.Print(err)
			}
			break
		}
		len := int32(binary.BigEndian.Uint32(tmp))
		t.broker.loger.Printf("readLen %d ", len);
		requestData := make([]byte, len)
		_, err = io.ReadFull(client.Reader, requestData) //读取内容
		if err != nil {
			if err == io.EOF {
				t.broker.loger.Print("EOF")
			} else {
				t.broker.loger.Print(err)
			}
			break
		}
		request := &protocol.Request{}
		err = proto.Unmarshal(requestData, request)
		if err != nil {
			t.broker.loger.Print("Unmarshal error %s", err);
		}else{
			t.broker.loger.Printf("receive request Key:%s : %s", request.Key, request);
		}

		switch request.Key {
		case protocol.RequestKey_CreatTopic:
			t.creatTopic(request)
		case protocol.RequestKey_GetPublisherPartition:
			t.getPublisherPartition(client)
		case protocol.RequestKey_Publish:
			t.publish(client)
		case protocol.RequestKey_GetConsumerPartition:
			t.getConsumerPartition(client)
		case protocol.RequestKey_Subscribe:
			t.subscribe(client)
		default:
			t.broker.loger.Print("cannot find key");
		}

	}
}
func (t *tcpServer) creatTopic(request *protocol.Request)  error{
	topicName := request.Topic
	partionNum := request.PartitionNum
	_, ok := t.broker.topicMap[topicName]
	if ok{
		err := errors.New("existed topic")
		return err
	}else{
		t.broker.loger.Printf("create topic : %s %d", topicName, int(partionNum))
		topic := newTopic(topicName, int(partionNum), t.broker)
		t.broker.topicMap[topicName] = topic
	}
	return nil
}

func (t *tcpServer) getPublisherPartition(client *client)  {

}
func (t *tcpServer) publish(client *client)  {

}
func (t *tcpServer) getConsumerPartition(client *client)  {

}
func (t *tcpServer) subscribe(client *client)  {

}
func (t *tcpServer) writeLoop(conn *client)  {

	return
}

