package consumer

import (
	"../mylib/etcdClient"
	"../mylib/myLogger"
	"../mylib/protocalFuc"
	"../protocol"
	"errors"
	"github.com/golang/protobuf/proto"
	"sync"
)
const logDir string = "./consumer/log/"
const openCluster bool = true
type Consumer struct {
	addrs   []string
	groupName string
	controllerConn *consumerConn
	partitions []*protocol.Partition
	brokerConnMapLock sync.RWMutex
	brokerConnMap map[string] *consumerConn  //broker ip映射到consumerConn
	sendIdx int
	readChan     chan *readData
	exitChan chan string
	etcdClient *etcdClient.EtcdClient
	topic string
}
type readData struct {
	connName string
	server2ClientData *protocol.Server2Client
}
type Handler interface {
	ProcessMsg(message *protocol.Server2Client)
}



func NewConsumer(addr []string, groupName string) (*Consumer, error) {
	_, err := myLogger.New(logDir)
	c := &Consumer{
		addrs: addr,
		groupName: groupName,
		readChan: make(chan *readData),
		exitChan: make(chan string),
		brokerConnMap: make(map[string] *consumerConn),
	}
	//err = c.Connect2Brokers()
	if openCluster{
		c.etcdClient, err = etcdClient.NewEtcdClient(c, false)
		if err != nil{
			myLogger.Logger.PrintError(err)
			return nil, err
		}
		err = c.connect2Controller() //连接到controller
		if err != nil{
			myLogger.Logger.PrintError(err)
			return nil, err
		}
	}else{
		err = c.Connect2Brokers()
	}
	return c, err
}

func (c *Consumer) connect2Controller() error{
	masterAddr, err := c.etcdClient.GetControllerAddr() //获取controller地址
	if err != nil {
		myLogger.Logger.PrintError(err)
	}
	err = c.Connect2Broker(masterAddr.ClientListenAddr)
	if err != nil {
		myLogger.Logger.PrintError(err)
	}
	myLogger.Logger.Print("connect2Controllerrer", masterAddr.ClientListenAddr)
	c.controllerConn, _ = c.getBrokerConn(&masterAddr.ClientListenAddr)
	return err
}

func (c *Consumer) ChangeControllerAddr(addr *protocol.ListenAddr) {
	masterAddr := addr.ClientListenAddr
	err := c.Connect2Broker(masterAddr)
	if err != nil {
		myLogger.Logger.PrintError(err)
	}
	c.controllerConn, _ = c.getBrokerConn(&masterAddr)
	c.SubscribeTopic(c.topic)
	return
}
func (c *Consumer) BecameNormalBroker() {
	myLogger.Logger.PrintError("should not come here")
	return
}
func (c *Consumer) BecameController() {
	myLogger.Logger.PrintError("should not come here")
	return
}

func (c *Consumer) addBrokerConn(conn *consumerConn) {
	c.brokerConnMapLock.Lock()
	c.brokerConnMap[conn.addr] = conn
	c.brokerConnMapLock.Unlock()
	return
}

func (c *Consumer) getBrokerConn(addr *string) (*consumerConn, bool) {

	myLogger.Logger.Print("getBrokerConn", addr)
	c.brokerConnMapLock.RLock()
	broker, ok := c.brokerConnMap[*addr]
	c.brokerConnMapLock.RUnlock()
	return broker, ok
}
func (c *Consumer) removeBrokerConn(conn *consumerConn) {
	myLogger.Logger.Print("removeBrokerConn", conn.addr)
	c.brokerConnMapLock.Lock()
	_, ok := c.brokerConnMap[conn.addr]
	if !ok {
		c.brokerConnMapLock.Unlock()
		return
	}
	delete(c.brokerConnMap, conn.addr)
	c.brokerConnMapLock.Unlock()
}

func (c *Consumer) Connect2Brokers() error {
	myLogger.Logger.Print("Connect2Brokers", c.addrs)
	for _, addr := range c.addrs {
		err := c.Connect2Broker(addr)
		if err != nil {
			return err
		}
	}
	return nil
}

func (c *Consumer) Connect2Broker(addr string) error {
	if _, ok := c.getBrokerConn(&addr); ok{
		myLogger.Logger.Printf("connecting to existing broker - %s", addr)
		return nil
	}
	var err error
	c.controllerConn, err = newConn(addr, c)
	if err != nil {
		myLogger.Logger.Printf("(%s) connecting to broker error - %s %s", addr, err)
		return err
	}
	go c.controllerConn.Handle()
	myLogger.Logger.Printf("connecting to broker - %s", addr)
	c.addBrokerConn(c.controllerConn)
	return nil
}

func (c *Consumer) SubscribeTopic(topicName string) error {
	c.topic = topicName
	requestData := &protocol.SubscribeTopicReq{
		TopicName: topicName,
		GroupName: c.groupName,
	}
	data, err := proto.Marshal(requestData)
	if err != nil {
		myLogger.Logger.PrintError("marshaling error", err)
		return err
	}
	reqData, err := protocalFuc.PackClientServerProtoBuf(protocol.ClientServerCmd_CmdSubscribeTopicReq, data)
	if err != nil {
		myLogger.Logger.PrintError("marshaling error", err)
		return err
	}
	myLogger.Logger.Printf("write: %s", requestData)
	c.controllerConn.Put(reqData)
	if err != nil {
		myLogger.Logger.PrintError("controllerConn Write err", err)
		return err
	}


	//c.topic = topicName
	//requestData := &protocol.Client2Server{
	//	Key:       protocol.Client2ServerKey_SubscribeTopic,
	//	Topic:     topicName,
	//	GroupName: c.groupName,
	//}
	//c.controllerConn.Put(requestData)
	//c.controllerConn.writeChan <- requestData
	myLogger.Logger.Printf("subscribeTopic %s : %s", topicName, requestData)
	return nil
}

func (c *Consumer) CommitReadyNum(num int32) error {
	c.brokerConnMapLock.RLock()
	defer c.brokerConnMapLock.RUnlock()
	if len(c.brokerConnMap) == 0{
		return errors.New("do not have any brokerConn")
	}


	requestData := &protocol.CommitReadyNumReq{
		ReadyNum: num / int32(len(c.brokerConnMap)),
	}
	data, err := proto.Marshal(requestData)
	if err != nil {
		myLogger.Logger.PrintError("marshaling error", err)
		return err
	}
	reqData, err := protocalFuc.PackClientServerProtoBuf(protocol.ClientServerCmd_CmdCommitReadyNumReq, data)
	if err != nil {
		myLogger.Logger.PrintError("marshaling error", err)
		return err
	}

	c.brokerConnMapLock.RLock()
	for _, brokerConn := range c.brokerConnMap{
		myLogger.Logger.Printf("write: %s", requestData)
		err = brokerConn.Put(reqData)
		if err != nil {
			myLogger.Logger.PrintError("controllerConn Write err", err)
			return err
		}
	}
	c.brokerConnMapLock.RUnlock()
	//requestData := &protocol.Client2Server{
	//	Key: protocol.Client2ServerKey_CommitReadyNum,
	//	ReadyNum: num / int32(len(c.brokerConnMap)),
	//}


	//for _, brokerConn := range c.brokerConnMap{
	//	brokerConn.Put(requestData)
	//}

	myLogger.Logger.Print("commitReadyNum", requestData)
	return nil
}


func (c *Consumer) ReadLoop(handler Handler) {
	for{
		select {
		case  <- c.exitChan:
			break
		case data := <- c.readChan:
			myLogger.Logger.Print("readLoop")

			server2ClientData := data.server2ClientData
			var response []byte
			switch server2ClientData.Key {
			case protocol.Server2ClientKey_PushMsg:
				if handler == nil{//为空则使用默认处理
					response = c.processMsg(server2ClientData)
				}else{//否则使用传入参数处理
					handler.ProcessMsg(server2ClientData)

					rsp := &protocol.PushMsgRsp{
						PartitionName: server2ClientData.MsgPartitionName,
						GroupName: server2ClientData.MsgGroupName,
						MsgId: server2ClientData.Msg.Id,
					}
					data, err := proto.Marshal(rsp)
					if err != nil {
						myLogger.Logger.PrintError("marshaling error", err)
						continue
					}
					reqData, err := protocalFuc.PackClientServerProtoBuf(protocol.ClientServerCmd_CmdPushMsgRsp, data)
					if err != nil {
						myLogger.Logger.PrintError("marshaling error", err)
						continue
					}
					myLogger.Logger.Printf("write: %s", rsp)
					response = reqData

					//response = &protocol.Client2Server{
					//	Key: protocol.Client2ServerKey_ConsumeSuccess,
					//	Partition: server2ClientData.MsgPartitionName,
					//	GroupName: server2ClientData.MsgGroupName,
					//	MsgId: server2ClientData.Msg.Id,
					//}
				}
			case protocol.Server2ClientKey_ChangeConsumerPartition:
				response = c.changeConsumerPartition(server2ClientData)
			case protocol.Server2ClientKey_Success:
				myLogger.Logger.Print("success")
			default:
				myLogger.Logger.Print("cannot find key :", server2ClientData.Key )
			}
			if response != nil { //ask
				conn, ok := c.getBrokerConn(&data.connName)
				if ok {
					//myLogger.Logger.Print("write response", response)
					conn.writeChan <- response
				}else{
					myLogger.Logger.Print("conn cannot find", data.connName)
				}
			}

		}

	}
}


func (c *Consumer) processMsg(data *protocol.Server2Client) ([]byte){
	myLogger.Logger.Print("Consumer receive data:", data.Msg.Msg)

	rsp := &protocol.PushMsgRsp{
		PartitionName: data.MsgPartitionName,
		GroupName: data.MsgGroupName,
		MsgId: data.Msg.Id,
	}
	data2, err := proto.Marshal(rsp)
	if err != nil {
		myLogger.Logger.PrintError("marshaling error", err)
		return nil
	}
	reqData, err := protocalFuc.PackClientServerProtoBuf(protocol.ClientServerCmd_CmdPushMsgRsp, data2)
	if err != nil {
		myLogger.Logger.PrintError("marshaling error", err)
		return nil
	}
	//response = reqData

	//response := &protocol.Client2Server{
	//	Key: protocol.Client2ServerKey_ConsumeSuccess,
	//	Partition: data.MsgPartitionName,
	//	GroupName: data.MsgGroupName,
	//	MsgId: data.Msg.Id,
	//}
	myLogger.Logger.Printf("write: %s", rsp)
	return reqData
}
func (c *Consumer) changeConsumerPartition(data *protocol.Server2Client) ([]byte){
	myLogger.Logger.Print("changeConsumerPartition:", data.Partitions)
	c.partitions = data.Partitions
	c.subscribePartion(data.RebalanceId)
	return nil
}


func (c *Consumer) subscribePartion(rebalanceId int32) error{
	myLogger.Logger.Print("subscribePartion len:", len(c.partitions))
	for _, partition := range c.partitions{
		requestData := &protocol.SubscribePartitionReq{
			PartitionName: partition.Name,
			GroupName: c.groupName,
			RebalanceId: rebalanceId,
		}
		data, err := proto.Marshal(requestData)
		if err != nil {
			myLogger.Logger.PrintError("marshaling error", err)
			return err
		}
		reqData, err := protocalFuc.PackClientServerProtoBuf(protocol.ClientServerCmd_CmdSubscribePartitionReq, data)
		if err != nil {
			myLogger.Logger.PrintError("marshaling error", err)
			return err
		}
		//requestData := &protocol.Client2Server{
		//	Key: protocol.Client2ServerKey_SubscribePartion,
		//	//Topic: partition.TopicName,
		//	Partition:   partition.Name,
		//	GroupName:   c.groupName,
		//	RebalanceId: rebalanceId,
		//}
		conn, ok := c.getBrokerConn(&partition.Addr)
		if ok {
			myLogger.Logger.Print("getBrokerConn success", partition.Addr)
		}else{
			err := c.Connect2Broker(partition.Addr)
			if err != nil {
				myLogger.Logger.PrintError("subscribePartion :", partition.Name, "err", err)
				continue
			}
			conn, _ = c.getBrokerConn(&partition.Addr)
			myLogger.Logger.Print("subscribePartion have not connect", partition.Addr)
		}

		myLogger.Logger.Printf("write: %s", requestData)
		err = conn.Put(reqData)
		if err != nil {
			myLogger.Logger.PrintError("subscribePartion :", partition.Name, "err", err)
			continue
		}
		//conn.writeChan <- requestData
	}
	return nil

}

//func (c *Consumer) subscribePartion(rebalanceId int32) error{
//	myLogger.Logger.Print("subscribePartion len:", len(c.partitions))
//	for _, partition := range c.partitions{
//		requestData := &protocol.Client2Server{
//			Key: protocol.Client2ServerKey_SubscribePartion,
//			//Topic: partition.TopicName,
//			Partition:   partition.Name,
//			GroupName:   c.groupName,
//			RebalanceId: rebalanceId,
//		}
//		conn, ok := c.getBrokerConn(&partition.Addr)
//		if ok {
//			myLogger.Logger.Print("subscribePartion success", requestData)
//		}else{
//			err := c.Connect2Broker(partition.Addr)
//			if err != nil {
//				myLogger.Logger.PrintError("subscribePartion :", partition.Name, "err", err)
//				continue
//			}
//			conn, _ = c.getBrokerConn(&partition.Addr)
//			myLogger.Logger.Print("subscribePartion have not connect", partition.Addr)
//		}
//
//		err := conn.Put(requestData)
//		if err != nil {
//			myLogger.Logger.PrintError("subscribePartion :", partition.Name, "err", err)
//			continue
//		}
//		//conn.writeChan <- requestData
//	}
//	return nil
//
//}

