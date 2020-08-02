package producer

import (
	"../mylib/myLogger"
	"../protocol"
	"encoding/binary"
	"errors"
	"github.com/golang/protobuf/proto"
	"log"
	"sync"
)

type Producer struct {
	addrs   []string
	//Addr   string
	conn *producerConn
	partitionMapLock sync.RWMutex
	partitionMap map[string] []*protocol.Partition//Partition名 映射到 protocol.Partition
	sendIdx int
	connectState int32
	brokerConnMapLock sync.RWMutex
	brokerConnMap map[string] *producerConn  //broker ip 映射到 producerConn
	readChan     chan *readData
	pubishAsk     chan string
}
type readData struct {
	connName string
	server2ClientData *protocol.Server2Client
}
const logDir string = "./producer/log/"
func NewProducer(addrs []string) (*Producer, error) {
	_, err := myLogger.New(logDir)
	if err != nil {
		return nil, err
	}
	p := &Producer{
		addrs: addrs,
		//Addr: addr,
		partitionMap: make(map[string] []*protocol.Partition),
		brokerConnMap: make(map[string] *producerConn ),
		readChan: make(chan *readData),
		pubishAsk: make(chan string),
	}
	err = p.Connect2Brokers()
	if err != nil {
		return nil, err
	}
	return p, err
}

func (p *Producer) addBrokerConn(conn *producerConn) {
	p.brokerConnMapLock.Lock()
	p.brokerConnMap[conn.addr] = conn
	p.brokerConnMapLock.Unlock()
	return
}

func (p *Producer) getBrokerConn(addr *string) (*producerConn, bool) {
	p.brokerConnMapLock.RLock()
	broker, ok := p.brokerConnMap[*addr]
	p.brokerConnMapLock.RUnlock()
	return broker, ok
}
func (p *Producer) removeBrokerConn(conn *producerConn) {
	p.brokerConnMapLock.Lock()
	_, ok := p.brokerConnMap[conn.addr]
	if !ok {
		p.brokerConnMapLock.Unlock()
		return
	}
	delete(p.brokerConnMap, conn.addr)
	p.brokerConnMapLock.Unlock()
}

func (p *Producer) Connect2Brokers() error {
	myLogger.Logger.Print("Connect2Brokers", p.addrs)
	for _, addr := range p.addrs {
		err := p.connect2Broker(addr)
		if err != nil {
			return err
		}
	}
	return nil
}
func (p *Producer) connect2Broker(addr string) error {
	if _, ok := p.getBrokerConn(&addr); ok{
		myLogger.Logger.Printf("connecting to existing broker - %s", addr)
		return nil
	}
	var err error
	p.conn, err = newConn(addr, p)

	if err != nil {
		//p.conn.Close()
		myLogger.Logger.Printf(" connecting to broker error: %s %s", addr, err)
		//atomic.StoreInt32(p.connectState)
		return err
	}
	//go p.conn.Handle()
	myLogger.Logger.Printf("connecting to broker - %s", addr)
	p.addBrokerConn(p.conn)
	return nil
}

func (p *Producer) CreatTopic(topic string, num int32){
	requestData := &protocol.Client2Server{
		Key: protocol.Client2ServerKey_CreatTopic,
		Topic: topic,
		PartitionNum: num,
	}
	//p.conn.writeChan <- requestData

	data, err := proto.Marshal(requestData)
	if err != nil {
		log.Fatal("marshaling error: ", err)
	}
	var buf [4]byte
	bufs := buf[:]
	binary.BigEndian.PutUint32(bufs, uint32(len(data)))
	p.conn.writer.Write(bufs)
	p.conn.writer.Write(data)
	p.conn.writer.Flush()
	myLogger.Logger.Printf("CreatTopic %s : %d", topic, num)

	response, err:= p.conn.readResponse()
	if err == nil{
		p.partitionMap[topic] = response.Partitions
	}
}

func (p *Producer) getPartition(topic string) (*protocol.Partition){ //循环读取
	p.partitionMapLock.RLock()
	defer p.partitionMapLock.RUnlock()
	partitions, ok := p.partitionMap[topic]
	if !ok {
		myLogger.Logger.Print("topic not exist :", topic)
		err := p.GetTopicPartition(topic)
		if err != nil {
			myLogger.Logger.Print("getPartition error: ", err)
			return nil
		}
	}
	partitions, ok = p.partitionMap[topic]
	if !ok {
		myLogger.Logger.Print("topic not exist :", topic)
		err := p.GetTopicPartition(topic)
		if err != nil {
			myLogger.Logger.Print("getPartition error: ", err)
			return nil
		}
	}

	len := len(partitions)
	if len == 0{
		myLogger.Logger.Print("getPartition err")
		return nil
	}
	p.sendIdx++
	if p.sendIdx >= len{
		p.sendIdx %= len
	}
	return partitions[p.sendIdx]
}
func (p *Producer) Pubilsh(topic string, data []byte, prioroty int32) error{
	msg := &protocol.Message{
		Priority: prioroty,
		Msg: data,
	}
	partition := p.getPartition(topic)
	if partition == nil{
		return errors.New("cannot get partition")
	}
	requestData := &protocol.Client2Server{
		Key: protocol.Client2ServerKey_Publish,
		Topic: topic,
		Partition:partition.Name,
		Msg: msg,
	}
	//p.conn.writeChan <- requestData
	data, err := proto.Marshal(requestData)
	if err != nil {
		log.Fatal("marshaling error: ", err)
	}

	p.conn.Write(data)
	if err != nil {
		myLogger.Logger.PrintError("writer error: ", err)
		return err
	}
	//p.conn.conn.Close()
	myLogger.Logger.Printf("Pubilsh %s", requestData)
	response, err:= p.conn.readResponse()
	if err == nil{
		if response.Key == protocol.Server2ClientKey_PublishSuccess{
			myLogger.Logger.Print("PublishSuccess")
		}
	}else{
		myLogger.Logger.Print("PublishError")
	}

	return nil
}

//func (p *Producer) PubilshAsync(topic string, data []byte, prioroty int32) error{
//	msg := &protocol.Message{
//		Priority: prioroty,
//		Msg: data,
//	}
//	partition := p.getPartition(topic)
//	if partition == nil{
//		return errors.New("cannot get partition")
//	}
//	requestData := &protocol.Client2Server{
//		Key: protocol.Client2ServerKey_Publish,
//		Topic: topic,
//		Partition:partition.Name,
//		Msg: msg,
//	}
//	p.conn.writeChan <- requestData
//	return nil
//}
//
//func (p *Producer) ReadLoop() {
//	for{
//
//		data := <- p.readChan
//		myLogger.Logger.Print("readLoop: ", data)
//		server2ClientData := data.server2ClientData
//		var response *protocol.Client2Server
//		switch server2ClientData.Key {
//		case protocol.Server2ClientKey_SendPartions:
//			p.partitionMap[server2ClientData.Topic] = server2ClientData.Partitions
//		case protocol.Server2ClientKey_PublishSuccess:
//
//		default:
//			myLogger.Logger.Print("cannot find key :", server2ClientData.Key )
//		}
//		if response != nil { //ask
//			conn, ok := p.getBrokerConn(&data.connName)
//			if ok {
//				myLogger.Logger.Print("write response", response)
//				conn.writeChan <- response
//			}else{
//				myLogger.Logger.Print("conn cannot find", data.connName)
//			}
//		}
//	}
//}


func (p *Producer) GetTopicPartition(topic string) error{
	requestData := &protocol.Client2Server{
		Key: protocol.Client2ServerKey_GetPublisherPartition,
		Topic: topic,
	}
	//p.conn.writeChan <- requestData

	data, err := proto.Marshal(requestData)
	if err != nil {
		myLogger.Logger.Print("marshaling error: ", err)
		return err
	}
	var buf [4]byte
	bufs := buf[:]
	binary.BigEndian.PutUint32(bufs, uint32(len(data)))
	p.conn.writer.Write(bufs)
	p.conn.writer.Write(data)
	p.conn.writer.Flush()
	myLogger.Logger.Printf("GetTopicPartion %s : %s", topic, requestData)

	response, err:= p.conn.readResponse()
	if err == nil{
		p.partitionMap[topic] = response.Partitions
	}
	return nil
}

