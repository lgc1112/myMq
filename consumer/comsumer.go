package consumer

import (
	"../mylib/myLogger"
	"../protocol"
	"sync"
)

type Consumer struct {
	addrs   []string
	groupName string
	conn *consumerConn
	partitions []*protocol.Partition
	brokerConnMapLock sync.RWMutex
	brokerConnMap map[string] *consumerConn  //broker ip映射到consumerConn
	sendIdx int
	readChan     chan *readData

}
type readData struct {
	connName string
	server2ClientData *protocol.Server2Client
}
type Handler interface {
	ProcessMsg(message *protocol.Server2Client)
}
//
//type HandlerFunc func(message *protocol.Server2Client) error
//
//
//func (h HandlerFunc) processMsg(m *protocol.Server2Client) error {
//	return h(m)
//}

const logDir string = "./consumer/log/"
func NewConsumer(addr []string, groupName string) (*Consumer, error) {
	_, err := myLogger.New(logDir)
	c := &Consumer{
		addrs: addr,
		groupName: groupName,
		readChan: make(chan *readData),
		brokerConnMap: make(map[string] *consumerConn),
	}
	err = c.Connect2Brokers()
	if err != nil {
		return nil, err
	}
	return c, err
}

func (c *Consumer) addBrokerConn(conn *consumerConn) {
	c.brokerConnMapLock.Lock()
	c.brokerConnMap[conn.addr] = conn
	c.brokerConnMapLock.Unlock()
	return
}

func (c *Consumer) getBrokerConn(addr *string) (*consumerConn, bool) {
	c.brokerConnMapLock.RLock()
	broker, ok := c.brokerConnMap[*addr]
	c.brokerConnMapLock.RUnlock()
	return broker, ok
}
func (c *Consumer) removeBrokerConn(conn *consumerConn) {
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
	c.conn, err = newConn(addr, c)
	if err != nil {
		myLogger.Logger.Printf("(%s) connecting to broker error - %s %s", addr, err)
		return err
	}
	go c.conn.Handle()
	myLogger.Logger.Printf("connecting to broker - %s", addr)
	c.addBrokerConn(c.conn)
	return nil
}

func (c *Consumer) SubscribeTopic(topicName string) error {
	requestData := &protocol.Client2Server{
		Key: protocol.Client2ServerKey_SubscribeTopic,
		Topic: topicName,
		GroupName: c.groupName,
	}
	c.conn.writeChan <- requestData
	myLogger.Logger.Printf("subscribeTopic %s : %s", topicName, requestData)
	return nil
}

func (c *Consumer) CommitReadyNum(num int32) error {
	requestData := &protocol.Client2Server{
		Key: protocol.Client2ServerKey_CommitReadyNum,
		ReadyNum: num,
	}
	c.conn.writeChan <- requestData
	myLogger.Logger.Print("commitReadyNum", requestData)
	return nil
}


func (c *Consumer) ReadLoop(handler Handler) {
	//testTime := 10 * time.Second
	//var i int64
	//timeTicker := time.NewTicker(testTime)
	//timeTickerChan := timeTicker.C

	for{
		select {
		//case <- timeTickerChan:
		//	myLogger.Logger.Print("readLoop timeout: testtime")
		//	timeTicker.Stop()
		//	return
		case data := <- c.readChan:
			myLogger.Logger.Print("readLoop")

			server2ClientData := data.server2ClientData
			var response *protocol.Client2Server
			switch server2ClientData.Key {
			case protocol.Server2ClientKey_PushMsg:
				if handler == nil{//为空则使用默认处理
					response = c.processMsg(server2ClientData)
				}else{//否则使用传入参数处理
					handler.ProcessMsg(server2ClientData)
					response = &protocol.Client2Server{
						Key: protocol.Client2ServerKey_ConsumeSuccess,
						Partition: server2ClientData.MsgPartitionName,
						GroupName: server2ClientData.MsgGroupName,
						MsgId: server2ClientData.Msg.Id,
					}
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


func (c *Consumer) ReadLoopInTime(handler Handler) {
	//testTime := 10 * time.Second
	//var i int64
	//timeTicker := time.NewTicker(testTime)
	//timeTickerChan := timeTicker.C

	for{
		select {
		//case <- timeTickerChan:
		//	myLogger.Logger.Print("readLoop timeout: testtime")
		//	timeTicker.Stop()
		//	return
		case data := <- c.readChan:
			myLogger.Logger.Print("readLoop")

			server2ClientData := data.server2ClientData
			var response *protocol.Client2Server
			switch server2ClientData.Key {
			case protocol.Server2ClientKey_PushMsg:
				if handler == nil{//为空则使用默认处理
					response = c.processMsg(server2ClientData)
				}else{//否则使用传入参数处理
					handler.ProcessMsg(server2ClientData)
					response = &protocol.Client2Server{
						Key: protocol.Client2ServerKey_ConsumeSuccess,
						Partition: server2ClientData.MsgPartitionName,
						GroupName: server2ClientData.MsgGroupName,
						MsgId: server2ClientData.Msg.Id,
					}
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
					myLogger.Logger.Print("write response", response)
					conn.writeChan <- response
				}else{
					myLogger.Logger.Print("conn cannot find", data.connName)
				}
			}

		}

	}
}

func (c *Consumer) processMsg(data *protocol.Server2Client) (*protocol.Client2Server){
	myLogger.Logger.Print("Consumer receive data:", data.Msg.Msg)
	response := &protocol.Client2Server{
		Key: protocol.Client2ServerKey_ConsumeSuccess,
		Partition: data.MsgPartitionName,
		GroupName: data.MsgGroupName,
		MsgId: data.Msg.Id,
	}
	return response
}
func (c *Consumer) changeConsumerPartition(data *protocol.Server2Client) (*protocol.Client2Server){
	myLogger.Logger.Print("changeConsumerPartition:", data.Partitions)
	c.partitions = data.Partitions
	c.subscribePartion(data.RebalanceId)
	return nil
}

func (c *Consumer) subscribePartion(rebalanceId int32){
	myLogger.Logger.Print("subscribePartion len:", len(c.partitions))
	for _, partition := range c.partitions{
		requestData := &protocol.Client2Server{
			Key: protocol.Client2ServerKey_SubscribePartion,
			//Topic: partition.TopicName,
			Partition: partition.Name,
			GroupName: c.groupName,
			RebalanceId: rebalanceId,
		}
		conn, ok := c.getBrokerConn(&partition.Addr)
		if ok {
			myLogger.Logger.Print("subscribePartion success", requestData)
			conn.writeChan <- requestData
		}else{
			myLogger.Logger.Print("subscribePartion have not connect", partition.Addr)
		}
	}


}

