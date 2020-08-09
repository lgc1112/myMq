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

const logDir string = "./consumerlog/"
const openCluster bool = true                     //是否开启集群模式
const defaultEtcdAddr string = "9.135.8.253:2379" //etcd服务器地址
type Consumer struct {
	addrs     []string
	groupName string
	//controllerConnLock sync.RWMutex
	controllerConn    *consumerConn
	partitions        []*protocol.Partition
	brokerConnMapLock sync.RWMutex
	brokerConnMap     map[string]*consumerConn //broker ip映射到consumerConn
	sendIdx           int
	readChan          chan *readData
	exitChan          chan string
	topicResponseChan chan bool
	etcdClient        *etcdClient.ClientEtcdClient
	topic             string
}
type readData struct {
	connAddr string
	cmd      *protocol.ClientServerCmd
	msgBody  []byte
	//server2ClientData *protocol.Server2Client
}
type Handler interface {
	ProcessMsg(msg *protocol.Message)
}

// 新建消费者实例
func NewConsumer(addr []string, groupName string) (*Consumer, error) {
	_, err := myLogger.New(logDir)
	c := &Consumer{
		addrs:             addr,
		groupName:         groupName,
		readChan:          make(chan *readData),
		exitChan:          make(chan string),
		topicResponseChan: make(chan bool),
		brokerConnMap:     make(map[string]*consumerConn),
	}
	if openCluster {
		etcdAddr := defaultEtcdAddr
		c.etcdClient, err = etcdClient.NewClientEtcdClient(c, &etcdAddr)
		if err != nil {
			myLogger.Logger.PrintError(err)
			return nil, err
		}
		err = c.connect2Controller() //连接到controller
		if err != nil {
			myLogger.Logger.PrintError(err)
			return nil, err
		}
	} else {
		err = c.Connect2Brokers()
	}
	return c, err
}

//连接到controller
func (c *Consumer) connect2Controller() error {
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

//controller服务器发生改变时的回调函数
func (c *Consumer) ControllerAddrChange(addr *protocol.ListenAddr) {
	myLogger.Logger.Print("ControllerAddrChange: ", addr)
	controller := addr.ClientListenAddr
	err := c.Connect2Broker(controller)
	if err != nil {
		myLogger.Logger.PrintError(err)
	}
	c.controllerConn, _ = c.getBrokerConnAndLock(&controller)
	c.SubscribeTopic(c.topic)
	c.getBrokerConnUnLock()
	return
}

//集群中某topic发生变化时的回调函数
func (c *Consumer) TopicChange(topic string, partition *protocol.Partitions) {
	if c.controllerConn == nil {
		myLogger.Logger.Print("TopicChange when c.controllerConn == nil")
		return
	}
	myLogger.Logger.Print("TopicChange: ", topic, partition)
	if topic == c.topic {
		if _, ok := c.getBrokerConnAndLock(&c.controllerConn.addr); ok { //查询连接是否失效
			c.SubscribeTopic(c.topic)
		}
		c.getBrokerConnUnLock()
	}
	return
}

//添加broker到brokerConnMap
func (c *Consumer) addBrokerConn(conn *consumerConn) {
	c.brokerConnMapLock.Lock()
	c.brokerConnMap[conn.addr] = conn
	c.brokerConnMapLock.Unlock()
	return
}

//获取broker的连接
func (c *Consumer) getBrokerConn(addr *string) (*consumerConn, bool) {
	myLogger.Logger.Print("getBrokerConn ", *addr)
	c.brokerConnMapLock.RLock()
	broker, ok := c.brokerConnMap[*addr]
	c.brokerConnMapLock.RUnlock()
	myLogger.Logger.Print("getBrokerConn end")
	return broker, ok
}

//获取broker的连接并锁住
func (c *Consumer) getBrokerConnAndLock(addr *string) (*consumerConn, bool) {
	myLogger.Logger.Print("getBrokerConn ", *addr)
	c.brokerConnMapLock.RLock()
	broker, ok := c.brokerConnMap[*addr]
	myLogger.Logger.Print("getBrokerConn end")
	return broker, ok
}

func (c *Consumer) getBrokerConnUnLock() {
	c.brokerConnMapLock.RUnlock()
}

//删除broker
func (c *Consumer) deleteBrokerConn(conn *consumerConn) {
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

//连接到所有c.addrs 中的broker地址
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

//连接到指定broker
func (c *Consumer) Connect2Broker(addr string) error {
	if _, ok := c.getBrokerConn(&addr); ok {
		myLogger.Logger.Printf("connecting to existing broker - %s", addr)
		return nil
	}
	var err error
	c.controllerConn, err = NewConn(addr, c)
	if err != nil {
		myLogger.Logger.Printf("(%s) connecting to broker error - %s %s", addr, err)
		return err
	}
	go c.controllerConn.Handle()
	myLogger.Logger.Printf("connecting to broker - %s", addr)
	c.addBrokerConn(c.controllerConn)
	return nil
}

//删除topic
func (c *Consumer) DeleteTopic(topic string) {
	if c.controllerConn == nil {
		myLogger.Logger.PrintError("controllerConn Not exist")
		return
	}
	requestData := &protocol.DeleteTopicReq{
		TopicName: topic,
	}
	data, err := proto.Marshal(requestData)
	if err != nil {
		myLogger.Logger.PrintError("marshaling error", err)
		return
	}
	reqData, err := protocalFuc.PackClientServerProtoBuf(protocol.ClientServerCmd_CmdDeleteTopicReq, data)
	if err != nil {
		myLogger.Logger.PrintError("marshaling error", err)
		return
	}
	if _, ok := c.getBrokerConnAndLock(&c.controllerConn.addr); ok { //查询连接是否失效
		err = c.controllerConn.Put(reqData)
	}
	c.getBrokerConnUnLock()

	if err != nil {
		myLogger.Logger.PrintError("controllerConn Write err", err)
		return
	}
	myLogger.Logger.Printf("DeleteTopic %s ", topic)
	succ := <-c.topicResponseChan
	myLogger.Logger.Print("DeleteTopic  ", succ)
}

//创建topic
func (c *Consumer) CreatTopic(topic string, num int32) {
	if c.controllerConn == nil {
		myLogger.Logger.PrintError("controllerConn Not exist")
		return
	}
	requestData := &protocol.CreatTopicReq{
		TopicName:    topic,
		PartitionNum: num,
	}
	data, err := proto.Marshal(requestData)
	if err != nil {
		myLogger.Logger.PrintError("marshaling error", err)
		return
	}
	reqData, err := protocalFuc.PackClientServerProtoBuf(protocol.ClientServerCmd_CmdCreatTopicReq, data)
	if err != nil {
		myLogger.Logger.PrintError("marshaling error", err)
		return
	}
	if _, ok := c.getBrokerConnAndLock(&c.controllerConn.addr); ok { //查询连接是否失效
		err = c.controllerConn.Put(reqData)
	}
	c.getBrokerConnUnLock()
	//err = c.controllerConn.Write(reqData)
	if err != nil {
		myLogger.Logger.PrintError("controllerConn Write err", err)
		return
	}
	myLogger.Logger.Printf("CreatTopic %s : %d", topic, num)
	succ := <-c.topicResponseChan
	myLogger.Logger.Print("CreatTopic ", succ)
}

//订阅topic
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

	if _, ok := c.getBrokerConnAndLock(&c.controllerConn.addr); ok { //查询连接是否失效
		err = c.controllerConn.Put(reqData)
	}
	c.getBrokerConnUnLock()
	if err != nil {
		myLogger.Logger.PrintError("controllerConn Write err", err)
		return err
	}
	myLogger.Logger.Printf("subscribeTopic %s : %s", topicName, requestData)
	return nil
}

//上传readyCount
func (c *Consumer) CommitReadyNum(num int32) error {
	c.brokerConnMapLock.RLock()
	defer c.brokerConnMapLock.RUnlock()
	if len(c.brokerConnMap) == 0 {
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
	for _, brokerConn := range c.brokerConnMap {
		myLogger.Logger.Printf("write: %s", requestData)
		err = brokerConn.Put(reqData)
		if err != nil {
			myLogger.Logger.PrintError("controllerConn Write err", err)
			return err
		}
	}
	myLogger.Logger.Print("commitReadyNum", requestData)
	return nil
}

//处理broker发来的消息
func (c *Consumer) ReadLoop(handler Handler, exitChan <-chan bool) {
	for {
		select {
		case _, ok := <-exitChan:
			if !ok {
				myLogger.Logger.Print("readLoop exit")
				return
			}
		case data := <-c.readChan:
			myLogger.Logger.Print("readLoop")

			cmd := data.cmd
			var response []byte
			switch *cmd {
			case protocol.ClientServerCmd_CmdPushMsgReq:
				req := &protocol.PushMsgReq{}
				err := proto.Unmarshal(data.msgBody, req) //得到消息体
				if err != nil {
					myLogger.Logger.PrintError("Unmarshal error %s", err)
					break
				} else {
					myLogger.Logger.Printf("receive PushMsgReq: %s", req)
				}
				if handler == nil { //为空则使用默认处理
					c.processMsg(req.Msg)
				} else { //否则使用传入参数处理
					handler.ProcessMsg(req.Msg)
				}
				rsp := &protocol.PushMsgRsp{
					Ret:           protocol.RetStatus_Successs,
					PartitionName: req.PartitionName,
					GroupName:     req.GroupName,
					MsgId:         req.Msg.Id,
				}
				//data, err := proto.Marshal(rsp)
				//if err != nil {
				//	myLogger.Logger.PrintError("marshaling error", err)
				//	break
				//}
				//reqData, err := protocalFuc.PackClientServerProtoBuf(protocol.ClientServerCmd_CmdPushMsgRsp, data)
				//if err != nil {
				//	myLogger.Logger.PrintError("marshaling error", err)
				//	break
				//}
				//myLogger.Logger.Printf("write: %s", rsp)
				//response = reqData
				conn, ok := c.getBrokerConnAndLock(&data.connAddr)
				if ok {
					myLogger.Logger.Print("write response", rsp)
					conn.writeMsgChan <- rsp
					//err := conn.PutMsg(rsp)
					//if err != nil {
					//	myLogger.Logger.PrintError("PutMsg error %s", err)
					//	c.getBrokerConnUnLock()
					//	continue
					//}
					myLogger.Logger.Print("write response end")
				} else {
					myLogger.Logger.Print("conn cannot find", data.connAddr)
				}
				c.getBrokerConnUnLock()

			case protocol.ClientServerCmd_CmdChangeConsumerPartitionReq:

				req := &protocol.ChangeConsumerPartitionReq{}
				err := proto.Unmarshal(data.msgBody, req) //得到消息体
				if err != nil {
					myLogger.Logger.PrintError("Unmarshal error %s", err)
					break
				} else {
					myLogger.Logger.Printf("receive ChangeConsumerPartitionReq: %s", req)
				}
				response = c.changeConsumerPartition(req.Partitions, req.RebalanceId)

			case protocol.ClientServerCmd_CmdSubscribePartitionRsp:
				req := &protocol.SubscribePartitionRsp{}
				err := proto.Unmarshal(data.msgBody, req) //得到消息体
				if err != nil {
					myLogger.Logger.PrintError("Unmarshal error %s", err)
					break
				} else {
					myLogger.Logger.Printf("receive ChangeConsumerPartition: %s", req)
				}
			case protocol.ClientServerCmd_CmdCreatTopicRsp:
				req := &protocol.CreatTopicRsp{}
				err := proto.Unmarshal(data.msgBody, req) //得到消息体
				myLogger.Logger.Print("CreatTopic response", req)
				if err != nil {
					myLogger.Logger.PrintError("Unmarshal error %s", err)
					return
				}
				if req.Ret == protocol.RetStatus_Successs { //成功创建
					c.topicResponseChan <- true
				} else {
					c.topicResponseChan <- false
				}

			case protocol.ClientServerCmd_CmdDeleteTopicRsp:
				req := &protocol.DeleteTopicRsp{}
				err := proto.Unmarshal(data.msgBody, req) //得到消息体
				if err != nil {
					myLogger.Logger.PrintError("Unmarshal error %s", err)
					return
				}
				if req.Ret == protocol.RetStatus_Successs {
					c.topicResponseChan <- true
				} else {
					c.topicResponseChan <- false
				}
				myLogger.Logger.Print("DeleteTopic response ", req)
			default:
				myLogger.Logger.Print("received key :", cmd)
			}
			if response != nil { //ask
				conn, ok := c.getBrokerConnAndLock(&data.connAddr)
				if ok {
					myLogger.Logger.Print("write response", response)
					//err := conn.Put(response)
					//if err != nil {
					//	myLogger.Logger.PrintError("Put error %s", err)
					//	c.getBrokerConnUnLock()
					//	return
					//}
					conn.writeChan <- response
					myLogger.Logger.Print("write response end")
				} else {
					myLogger.Logger.Print("conn cannot find", data.connAddr)
				}
				c.getBrokerConnUnLock()
			}

		}

	}
}

//默认消息处理函数
func (c *Consumer) processMsg(msg *protocol.Message) {
	myLogger.Logger.Print("Consumer receive data:", msg)
}

//修改消费者订阅的分区
func (c *Consumer) changeConsumerPartition(Partitions []*protocol.Partition, rebalanceId int32) []byte {
	myLogger.Logger.Print("changeConsumerPartition:", Partitions, "id : ", rebalanceId)
	c.partitions = Partitions
	c.subscribePartition(rebalanceId)
	return nil
}

//func (c *Consumer) waitCreateResponse() bool{
//	return <- c.createTopicChan
//}

//订阅分区
func (c *Consumer) subscribePartition(rebalanceId int32) error {
	myLogger.Logger.Print("subscribePartition len:", len(c.partitions))
	for _, partition := range c.partitions {
		requestData := &protocol.SubscribePartitionReq{
			PartitionName: partition.Name,
			GroupName:     c.groupName,
			RebalanceId:   rebalanceId,
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
		//conn, ok := c.getBrokerConnAndLock(&partition.Addr)
		conn, ok := c.getBrokerConn(&partition.Addr)
		if ok {
			myLogger.Logger.Print("getBrokerConn success", partition.Addr)
		} else {
			err := c.Connect2Broker(partition.Addr)
			if err != nil {
				myLogger.Logger.PrintError("subscribePartition :", partition.Name, "err", err)
				//c.getBrokerConnUnLock()
				continue
			}
			conn, _ = c.getBrokerConn(&partition.Addr)
			myLogger.Logger.Print("subscribePartition have not connect", partition.Addr)
		}

		myLogger.Logger.Printf("write: %s", requestData)
		err = conn.Put(reqData)
		if err != nil {
			myLogger.Logger.PrintError("subscribePartion :", partition.Name, "err", err)
			//c.getBrokerConnUnLock()
			continue
		}
		//c.getBrokerConnUnLock()
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

//func (c *Consumer) ReadLoop(handler Handler) {
//	for{
//		select {
//		case  <- c.exitChan:
//			break
//		case data := <- c.readChan:
//			myLogger.Logger.Print("readLoop")
//
//			server2ClientData := data.server2ClientData
//			var response []byte
//			switch server2ClientData.Key {
//			case protocol.Server2ClientKey_PushMsg:
//				if handler == nil{//为空则使用默认处理
//					response = c.processMsg(server2ClientData)
//				}else{//否则使用传入参数处理
//					handler.ProcessMsg(server2ClientData)
//
//					rsp := &protocol.PushMsgRsp{
//						PartitionName: server2ClientData.MsgPartitionName,
//						GroupName: server2ClientData.MsgGroupName,
//						MsgId: server2ClientData.Msg.Id,
//					}
//					data, err := proto.Marshal(rsp)
//					if err != nil {
//						myLogger.Logger.PrintError("marshaling error", err)
//						continue
//					}
//					reqData, err := protocalFuc.PackClientServerProtoBuf(protocol.ClientServerCmd_CmdPushMsgRsp, data)
//					if err != nil {
//						myLogger.Logger.PrintError("marshaling error", err)
//						continue
//					}
//					myLogger.Logger.Printf("write: %s", rsp)
//					response = reqData
//
//					//response = &protocol.Client2Server{
//					//	Key: protocol.Client2ServerKey_ConsumeSuccess,
//					//	Partition: server2ClientData.MsgPartitionName,
//					//	GroupName: server2ClientData.MsgGroupName,
//					//	MsgId: server2ClientData.Msg.Id,
//					//}
//				}
//			case protocol.Server2ClientKey_ChangeConsumerPartition:
//				response = c.changeConsumerPartition(server2ClientData)
//			case protocol.Server2ClientKey_Success:
//				myLogger.Logger.Print("success")
//			default:
//				myLogger.Logger.Print("cannot find key :", server2ClientData.Key )
//			}
//			if response != nil { //ask
//				conn, ok := c.getBrokerConn(&data.connName)
//				if ok {
//					//myLogger.Logger.Print("write response", response)
//					conn.writeChan <- response
//				}else{
//					myLogger.Logger.Print("conn cannot find", data.connName)
//				}
//			}
//
//		}
//
//	}
//}
