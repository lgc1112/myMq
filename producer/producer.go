package producer

import (
	"../mylib/etcdClient"
	"../mylib/myLogger"
	"../protocol"
	"encoding/binary"
	"errors"
	"github.com/golang/protobuf/proto"
	"log"
	"sync"
)
const openCluster bool = true
const logDir string = "./producer/log/"
type Producer struct {
	addrs   []string
	//Addr   string
	controllerConn *producerConn //到master的连接
	partitionMapLock sync.RWMutex
	partitionMap map[string] []*protocol.Partition//Partition名 映射到 protocol.Partition
	sendIdx int
	connectState int32
	brokerConnMapLock sync.RWMutex
	brokerConnMap map[string] *producerConn  //broker ip 映射到 producerConn
	readChan     chan *readData
	pubishAsk     chan string
	etcdClient *etcdClient.EtcdClient
	controllerChange bool
}
type readData struct {
	connName string
	server2ClientData *protocol.Server2Client
}
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
	//err = p.Connect2Brokers()

	if openCluster{
		p.etcdClient, err = etcdClient.NewEtcdClient(p, false)
		if err != nil{
			myLogger.Logger.PrintError(err)
			return nil, err
		}
		err = p.connect2Controller() //连接到controller
		if err != nil{
			myLogger.Logger.PrintError(err)
			return nil, err
		}
	}
	if err != nil {
		return nil, err
	}
	return p, err
}

func (p *Producer) connect2Controller() error{
	masterAddr, err := p.etcdClient.GetControllerAddr() //获取controller地址
	if err != nil {
		myLogger.Logger.PrintError(err)
	}
	err = p.connect2Broker(masterAddr.ClientListenAddr)
	if err != nil {
		myLogger.Logger.PrintError(err)
	}
	myLogger.Logger.Print("connect2Controllerrer", masterAddr.ClientListenAddr)
	p.controllerConn, _ = p.getBrokerConn(&masterAddr.ClientListenAddr)
	return err
}

func (p *Producer) ChangeControllerAddr(addr *protocol.ListenAddr) {
	masterAddr := addr.ClientListenAddr
	err := p.connect2Broker(masterAddr)
	if err != nil {
		myLogger.Logger.PrintError(err)
	}
	p.controllerConn, _ = p.getBrokerConn(&masterAddr)
	p.controllerChange = true

	return
}
func (p *Producer) BecameNormalBroker() {
	myLogger.Logger.PrintError("should not come here")
	return
}
func (p *Producer) BecameController() {
	myLogger.Logger.PrintError("should not come here")
	return
}
func (p *Producer) addBrokerConn(conn *producerConn) {
	p.brokerConnMapLock.Lock()
	p.brokerConnMap[conn.addr] = conn
	myLogger.Logger.Print("addBrokerConn, current Len", len(p.brokerConnMap))
	p.brokerConnMapLock.Unlock()
	return
}

func (p *Producer) getBrokerConn(addr *string) (*producerConn, bool) {
	p.brokerConnMapLock.RLock()
	defer p.brokerConnMapLock.RUnlock()
	broker, ok := p.brokerConnMap[*addr]
	return broker, ok
}
func (p *Producer) removeBrokerConn(addr *string) {
	p.brokerConnMapLock.Lock()
	defer p.brokerConnMapLock.Unlock()
	_, ok := p.brokerConnMap[*addr]
	if !ok {
		return
	}
	delete(p.brokerConnMap, *addr)
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
	Conn, err := newConn(addr, p)

	if err != nil {
		//p.conn.Close()
		myLogger.Logger.Printf(" connecting to broker error: %s %s", addr, err)
		//atomic.StoreInt32(p.connectState)
		return err
	}
	//go p.conn.Handle()
	myLogger.Logger.Printf("connecting to broker - %s", addr)
	p.addBrokerConn(Conn)
	return nil
}


func (p *Producer) DeleteTopic(topic string){
	if p.controllerConn == nil{
		myLogger.Logger.PrintError("controllerConn Not exist")
		return
	}
	requestData := &protocol.Client2Server{
		Key: protocol.Client2ServerKey_DeleteTopic,
		Topic: topic,
	}
	data, err := proto.Marshal(requestData)
	if err != nil {
		log.Fatal("marshaling error: ", err)
	}

	err = p.controllerConn.Write(data)
	if err != nil {
		myLogger.Logger.PrintError("controllerConn Write err", err)
		return
	}
	myLogger.Logger.Printf("DeleteTopic %s ", topic)

	response, err:= p.controllerConn.readResponse()
	if err != nil {
		myLogger.Logger.PrintError("controllerConn readResponse err", err)
		return
	}
	if response.Key == protocol.Server2ClientKey_Success{
		delete(p.partitionMap, topic)
	}
}

func (p *Producer) CreatTopic(topic string, num int32){
	if p.controllerConn == nil{
		myLogger.Logger.PrintError("controllerConn Not exist")
		return
	}
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

	err = p.controllerConn.Write(data)
	if err != nil {
		myLogger.Logger.PrintError("controllerConn Write err", err)
		return
	}
	myLogger.Logger.Printf("CreatTopic %s : %d", topic, num)

	response, err:= p.controllerConn.readResponse()
	if err == nil{
		p.partitionMap[topic] = response.Partitions
	}
}

func (p *Producer) getPartition(topic string) (*protocol.Partition, error){ //循环读取
	if p.controllerConn == nil{
		myLogger.Logger.PrintError("controllerConn Not exist")
		return nil, errors.New("controllerConn Not exist")
	}
	if p.controllerChange{
		p.controllerChange = false
		myLogger.Logger.Print("topic not exist :", topic)
		err := p.GetTopicPartition(topic)
		if err != nil {
			myLogger.Logger.Print("getPartition error: ", err)
			return nil,err
		}
	}
	p.partitionMapLock.RLock()
	defer p.partitionMapLock.RUnlock()
	partitions, ok := p.partitionMap[topic]
	if !ok {
		myLogger.Logger.Print("topic not exist :", topic)
		err := p.GetTopicPartition(topic)
		if err != nil {
			myLogger.Logger.Print("getPartition error: ", err)
			return nil, err
		}
	}
	len := len(partitions)
	if len == 0{
		myLogger.Logger.Print("getPartition err")
		return nil, errors.New("do not have partition")
	}
	p.sendIdx++
	if p.sendIdx >= len{
		p.sendIdx %= len
	}
	return partitions[p.sendIdx], nil
}

func (p *Producer) PubilshHelper(topic string, partition *protocol.Partition, msg *protocol.Message) error{

	if partition == nil{
		return errors.New("cannot get partition")
	}
	brokerConn, ok := p.getBrokerConn(&partition.Addr)
	if !ok {
		err := p.connect2Broker(partition.Addr)
		if err != nil {
			return errors.New("cannot connect partition")
		}
		brokerConn, _ = p.getBrokerConn(&partition.Addr)
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
	err = brokerConn.Write(data)
	if err != nil {
		//myLogger.Logger.PrintError("writer error: ", err)
		return err
	}
	//p.conn.conn.Close()
	myLogger.Logger.Printf("Pubilsh %s", requestData)
	response, err:= brokerConn.readResponse()
	if err == nil{
		if response.Key == protocol.Server2ClientKey_PublishSuccess{
			myLogger.Logger.Print("PublishSuccess")
		}
	}else{
		//myLogger.Logger.Print("PublishError")
		return errors.New("PublishError")
	}
	return nil
}

func (p *Producer) Pubilsh(topic string, data []byte, prioroty int32) error{
	msg := &protocol.Message{
		Priority: prioroty,
		Msg: data,
	}

	partition, err := p.getPartition(topic)
	if err != nil{
		return err
	}
	err = p.PubilshHelper(topic, partition, msg)
	if err != nil{ //该分区所在已经失效，删除
		p.partitionMapLock.Lock()
		defer p.partitionMapLock.Unlock()
		p.removeBrokerConn(&partition.Addr)
		var tmp[]*protocol.Partition
		for _, par := range p.partitionMap[topic]{ //删除所有失效分区
			if par.Addr != partition.Addr{
				tmp = append(tmp, par)
			}
		}
		p.partitionMap[topic] = tmp
	}
	return err

}


//func (p *Producer) Pubilsh(topic string, data []byte, prioroty int32) error{
//	msg := &protocol.Message{
//		Priority: prioroty,
//		Msg: data,
//	}
//	partition := p.getPartition(topic)
//	if partition == nil{
//		return errors.New("cannot get partition")
//	}
//	brokerConn, ok := p.getBrokerConn(&partition.Addr)
//	if !ok {
//		err := p.connect2Broker(partition.Addr)
//		if err != nil {
//			return errors.New("cannot connect partition")
//		}
//		brokerConn, _ = p.getBrokerConn(&partition.Addr)
//	}
//
//	requestData := &protocol.Client2Server{
//		Key: protocol.Client2ServerKey_Publish,
//		Topic: topic,
//		Partition:partition.Name,
//		Msg: msg,
//	}
//	//p.conn.writeChan <- requestData
//	data, err := proto.Marshal(requestData)
//	if err != nil {
//		log.Fatal("marshaling error: ", err)
//	}
//	err = brokerConn.Write(data)
//	if err != nil {
//		myLogger.Logger.PrintError("writer error: ", err)
//		return err
//	}
//	//p.conn.conn.Close()
//	myLogger.Logger.Printf("Pubilsh %s", requestData)
//	response, err:= brokerConn.readResponse()
//	if err == nil{
//		if response.Key == protocol.Server2ClientKey_PublishSuccess{
//			myLogger.Logger.Print("PublishSuccess")
//		}
//	}else{
//		myLogger.Logger.Print("PublishError")
//		p.GetTopicPartition(topic)
//	}
//	return nil
//}
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
	if p.controllerConn == nil{
		myLogger.Logger.PrintError("controllerConn Not exist")
		return nil
	}
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
	p.controllerConn.writer.Write(bufs)
	p.controllerConn.writer.Write(data)
	p.controllerConn.writer.Flush()
	myLogger.Logger.Printf("GetTopicPartion %s : %s", topic, requestData)

	response, err:= p.controllerConn.readResponse()
	if err == nil{


		p.partitionMap[topic] = response.Partitions
	}
	return nil
}

