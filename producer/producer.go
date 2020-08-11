package producer

import (
	"../mylib/consistenthash"
	"../mylib/etcdClient"
	"../mylib/myLogger"
	"../mylib/protocalFuc"
	"../protocol"
	"errors"
	"github.com/golang/protobuf/proto"
	"sync"
)

const openCluster bool = true
const logDir string = "./producerlog/"
const defaultEtcdAddr string = "9.135.8.253:2379" //etcd服务器地址
const numVirtualNodes int = 3                     //一直性哈希的虚拟节点数
type Producer struct {
	addrs             []string      //broker的地址，不使用集群模式时需要传入
	controllerConn    *producerConn //保存到controller的连接
	partitionMapLock  sync.RWMutex
	partitionMap      map[string][]*protocol.Partition //topic名 映射到 protocol.Partition
	sendIdx           int
	connectState      int32
	brokerConnMapLock sync.RWMutex
	brokerConnMap     map[string]*producerConn //broker ip 映射到 producerConn
	pubishAsk         chan string
	etcdClient        *etcdClient.ClientEtcdClient //保存到etcd的连接
	consistenceHash   map[string]*consistenthash.ConsistenceHash
}

//新建生产者实例
func NewProducer(addrs []string) (*Producer, error) {
	_, err := myLogger.New(logDir)
	if err != nil {
		return nil, err
	}
	p := &Producer{
		addrs:           addrs,
		partitionMap:    make(map[string][]*protocol.Partition),
		brokerConnMap:   make(map[string]*producerConn),
		pubishAsk:       make(chan string),
		consistenceHash: make(map[string]*consistenthash.ConsistenceHash),
	}

	if openCluster {
		etcdAddr := defaultEtcdAddr
		p.etcdClient, err = etcdClient.NewClientEtcdClient(p, &etcdAddr)
		if err != nil {
			myLogger.Logger.PrintError(err)
			return nil, err
		}
		err = p.connect2Controller() //连接到controller
		if err != nil {
			myLogger.Logger.PrintError(err)
			return nil, err
		}
	} else {
		err = p.Connect2Brokers()
	}
	if err != nil {
		return nil, err
	}
	return p, err
}

//连接到controller服务器
func (p *Producer) connect2Controller() error {
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

//controller服务器发生改变时的回调函数
func (p *Producer) ControllerAddrChange(addr *protocol.ListenAddr) {
	masterAddr := addr.ClientListenAddr
	err := p.connect2Broker(masterAddr)
	if err != nil {
		myLogger.Logger.PrintError(err)
	}
	p.controllerConn, _ = p.getBrokerConn(&masterAddr)
	return
}

//集群中某topic发生变化时的回调函数
func (p *Producer) TopicChange(topic string, partition *protocol.Partitions) {
	myLogger.Logger.Print("TopicChange", topic, partition)
	p.partitionMapLock.RLock()
	defer p.partitionMapLock.RUnlock()
	if _, ok := p.partitionMap[topic]; !ok { //没有订阅
		return
	}
	//p.partitionMap[topic] = partition.Partition //更新分区，两种方式均可
	err := p.GetTopicPartition(topic) //更新分区
	if err != nil {
		myLogger.Logger.Print("getPartition error: ", err)
	}
	return
}

//添加broker连接
func (p *Producer) addBrokerConn(conn *producerConn) {
	p.brokerConnMapLock.Lock()
	p.brokerConnMap[conn.addr] = conn
	myLogger.Logger.Print("addBrokerConn, current Len", len(p.brokerConnMap))
	p.brokerConnMapLock.Unlock()
	return
}

//获取broker连接
func (p *Producer) getBrokerConn(addr *string) (*producerConn, bool) {
	p.brokerConnMapLock.RLock()
	defer p.brokerConnMapLock.RUnlock()
	broker, ok := p.brokerConnMap[*addr]
	return broker, ok
}

//关闭生产者的所有连接
func (p *Producer) Close() {
	p.brokerConnMapLock.RLock()
	defer p.brokerConnMapLock.RUnlock()
	for _, conn := range p.brokerConnMap {
		err := conn.Close()
		if err != nil {
			myLogger.Logger.PrintError("Close err :", conn.addr)
		}
	}
}

//删除broker连接
func (p *Producer) deleteBrokerConn(addr *string) {
	p.brokerConnMapLock.Lock()
	defer p.brokerConnMapLock.Unlock()
	_, ok := p.brokerConnMap[*addr]
	if !ok {
		return
	}
	delete(p.brokerConnMap, *addr)
}

//连接到所有broker
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

//连接到指定地址的broker
func (p *Producer) connect2Broker(addr string) error {
	if _, ok := p.getBrokerConn(&addr); ok {
		myLogger.Logger.Printf("connecting to existing broker - %s", addr)
		return nil
	}
	Conn, err := newConn(addr, p)
	if err != nil {
		//p.conn.Close()
		myLogger.Logger.Printf(" connecting to broker error: %s %s", addr, err)
		return err
	}
	myLogger.Logger.Printf("connecting to broker - %s", addr)
	p.addBrokerConn(Conn)
	return nil
}

//删除指定topic
func (p *Producer) DeleteTopic(topic string) {
	if p.controllerConn == nil {
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
	err = p.controllerConn.Write(reqData)
	if err != nil {
		myLogger.Logger.PrintError("controllerConn Write err", err)
		return
	}

	myLogger.Logger.Printf("DeleteTopic %s ", topic)

	cmd, msgBody, err := p.controllerConn.readResponse()
	if err != nil {
		myLogger.Logger.Print(err)
		return
	}
	if *cmd == protocol.ClientServerCmd_CmdDeleteTopicRsp {
		req := &protocol.DeleteTopicRsp{}
		err = proto.Unmarshal(msgBody, req) //得到消息体
		if err != nil {
			myLogger.Logger.PrintError("Unmarshal error %s", err)
			return
		}
		if req.Ret == protocol.RetStatus_Successs {
			delete(p.partitionMap, topic)
			p.consistenceHash[topic] = nil
		}
		myLogger.Logger.Print("DeleteTopic response ", req)
	}
}

//创建指定数量的topic
func (p *Producer) CreatTopic(topic string, num int32) {
	if p.controllerConn == nil {
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
	err = p.controllerConn.Write(reqData)
	if err != nil {
		myLogger.Logger.PrintError("controllerConn Write err", err)
		return
	}
	myLogger.Logger.Printf("CreatTopic %s : %d", topic, num)

	cmd, msgBody, err := p.controllerConn.readResponse()
	if err != nil {
		myLogger.Logger.Print(err)
		return
	}
	if *cmd == protocol.ClientServerCmd_CmdCreatTopicRsp {
		req := &protocol.CreatTopicRsp{}
		err = proto.Unmarshal(msgBody, req) //得到消息体
		if err != nil {
			myLogger.Logger.PrintError("Unmarshal error %s", err)
			return
		}
		if req.Ret == protocol.RetStatus_Successs { //成功获取分区
			p.partitionMap[topic] = req.Partitions
			p.changeConsistentHashMap(p.partitionMap[topic], topic)
		}

		myLogger.Logger.Print("CreatTopic response", req)
	}
}

//获取某topic的分区
func (p *Producer) GetTopicPartition(topic string) error {
	if p.controllerConn == nil {
		myLogger.Logger.PrintError("controllerConn Not exist")
		return nil
	}

	requestData := &protocol.GetPublisherPartitionReq{
		TopicName: topic,
	}
	data, err := proto.Marshal(requestData)
	if err != nil {
		myLogger.Logger.PrintError("marshaling error", err)
		return err
	}
	reqData, err := protocalFuc.PackClientServerProtoBuf(protocol.ClientServerCmd_CmdGetPublisherPartitionReq, data)
	if err != nil {
		myLogger.Logger.PrintError("marshaling error", err)
		return err
	}

	p.controllerConn.Write(reqData)
	myLogger.Logger.Printf("GetTopicPartion %s : %s", topic, requestData)

	cmd, msgBody, err := p.controllerConn.readResponse()
	if err != nil {
		myLogger.Logger.Print(err)
		return err
	}
	if *cmd == protocol.ClientServerCmd_CmdGetPublisherPartitionRsp {
		req := &protocol.GetPublisherPartitionRsp{}
		err = proto.Unmarshal(msgBody, req) //得到消息体
		if err != nil {
			myLogger.Logger.PrintError("Unmarshal error %s", err)
			return err
		} else {
			myLogger.Logger.Printf("receive GetConsumerPartitionReq: %s", req)
		}
		p.partitionMap[topic] = req.Partitions
		p.changeConsistentHashMap(p.partitionMap[topic], topic)
	}

	return nil
}

//修改一致性哈希表
func (p *Producer) changeConsistentHashMap(partitions []*protocol.Partition, topic string) {
	p.consistenceHash[topic] = consistenthash.New(numVirtualNodes)
	for _, par := range partitions {
		data, err := proto.Marshal(par)
		if err != nil {
			myLogger.Logger.PrintError("marshaling error", err)
			return
		}
		p.consistenceHash[topic].AddNode(string(data))
	}
}

//获取指定key的一致性哈希的分区
func (p *Producer) getConsistentPartittion(topic string, key string) (*protocol.Partition, error) {
	data, err := p.consistenceHash[topic].SearchNode(key)
	if err != nil {
		myLogger.Logger.PrintError(" error %s", err)
		return nil, err
	}
	partition := &protocol.Partition{}
	err = proto.Unmarshal([]byte(data), partition) //得到包头
	if err != nil {
		myLogger.Logger.PrintError("Unmarshal error %s", err)
		return nil, err
	}
	return partition, nil
}

//使用轮询的方式获取某topic的分区
func (p *Producer) getRandomPartition(topic string) (*protocol.Partition, error) { //循环读取
	if p.controllerConn == nil {
		myLogger.Logger.PrintError("controllerConn Not exist")
		return nil, errors.New("controllerConn Not exist")
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
	if len == 0 {
		myLogger.Logger.Print("getPartition err")
		return nil, errors.New("do not have partition")
	}
	p.sendIdx++ //轮询
	if p.sendIdx >= len {
		p.sendIdx %= len
	}
	return partitions[p.sendIdx], nil
}

//指定key指来获取一致性哈希分区
func (p *Producer) getPartitionbyKey(topic string, key string) (*protocol.Partition, error) { //循环读取
	if p.controllerConn == nil {
		myLogger.Logger.PrintError("controllerConn Not exist")
		return nil, errors.New("controllerConn Not exist")
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
	if len == 0 {
		myLogger.Logger.Print("getPartition err")
		return nil, errors.New("do not have partition")
	}

	par, err := p.getConsistentPartittion(topic, key)
	if err != nil {
		myLogger.Logger.PrintError("changeConsistentHashMap error", err)
		return nil, err
	}
	return par, nil
}

//同步方式发布消息
func (p *Producer) PubilshSync(partition *protocol.Partition, msg *protocol.Message) error {

	if partition == nil {
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

	requestData := &protocol.PublishReq{
		PartitionName: partition.Name,
		Msg:           msg,
	}
	data, err := proto.Marshal(requestData)
	if err != nil {
		myLogger.Logger.PrintError("marshaling error", err)
		return err
	}
	reqData, err := protocalFuc.PackClientServerProtoBuf(protocol.ClientServerCmd_CmdPublishReq, data)
	if err != nil {
		myLogger.Logger.PrintError("marshaling error", err)
		return err
	}
	err = brokerConn.Write(reqData)
	if err != nil {
		myLogger.Logger.PrintError("controllerConn Write err", err)
		return err
	}
	myLogger.Logger.Printf("Pubilsh %s", requestData)
	myLogger.Logger.PrintfDebug2("发布消息内容：%s   消息优先级：%d   目的分区 %s", requestData.Msg.Msg, requestData.Msg.Priority, requestData.PartitionName)
	//myLogger.Logger.PrintfDebug2("Pubilsh %s", requestData)

	cmd, msgBody, err := brokerConn.readResponse()
	if err != nil {
		myLogger.Logger.PrintError("requestData error", err)
		return err
	}
	if *cmd == protocol.ClientServerCmd_CmdGetPublisherPartitionRsp {
		req := &protocol.PushMsgRsp{}
		err = proto.Unmarshal(msgBody, req) //得到消息体
		if err != nil {
			myLogger.Logger.PrintError("Unmarshal error %s", err)
			return err
		} else {
			myLogger.Logger.Printf("receive GetConsumerPartitionReq: %s", req)
		}
		if req.Ret == protocol.RetStatus_Successs {
			myLogger.Logger.Print("PublishSuccess")
		} else {
			//myLogger.Logger.Print("PublishError")
			return errors.New("PublishError")
		}
	}
	return nil
}

//发布消息到topic
func (p *Producer) Pubilsh(topic string, data []byte, prioroty int32) error {
	msg := &protocol.Message{
		Priority: prioroty,
		Msg:      data,
	}
	partition, err := p.getRandomPartition(topic)
	if err != nil {
		return err
	}
	err = p.PubilshSync(partition, msg)
	if err != nil { //该分区所在broker已经失效，删除
		p.partitionMapLock.Lock()
		defer p.partitionMapLock.Unlock()
		p.deleteBrokerConn(&partition.Addr)
		var tmp []*protocol.Partition
		for _, par := range p.partitionMap[topic] { //删除所有失效分区
			if par.Addr != partition.Addr {
				tmp = append(tmp, par)
			}
		}
		p.partitionMap[topic] = tmp
		//p.changeConsistentHashMap(p.partitionMap[topic], topic)
		//p.consistenceHash[topic] = consistenthash.New(5)
		//for _, par := range tmp{
		//	p.consistenceHash[topic].AddNode(par.Name)
		//}
	}
	return err
}

//根据key一致性哈希后发布消息到topic
func (p *Producer) PubilshBykey(topic string, data []byte, prioroty int32, key string) error {
	msg := &protocol.Message{
		Priority: prioroty,
		Msg:      data,
	}
	partition, err := p.getPartitionbyKey(topic, key)
	if err != nil {
		return err
	}
	err = p.PubilshSync(partition, msg)
	if err != nil { //该分区所在已经失效，删除
		p.partitionMapLock.Lock()
		defer p.partitionMapLock.Unlock()
		p.deleteBrokerConn(&partition.Addr)
		var tmp []*protocol.Partition
		for _, par := range p.partitionMap[topic] { //删除所有失效分区
			if par.Addr != partition.Addr {
				tmp = append(tmp, par)
			}
		}
		p.partitionMap[topic] = tmp

		p.changeConsistentHashMap(p.partitionMap[topic], topic)
	}
	return err
}

//发布消息到指定分区
func (p *Producer) Pubilsh2Partition(topic string, data []byte, prioroty int32, partition *protocol.Partition) error {
	msg := &protocol.Message{
		Priority: prioroty,
		Msg:      data,
	}
	err := p.PubilshSync(partition, msg)
	if err != nil { //该分区所在已经失效，删除
		p.partitionMapLock.Lock()
		defer p.partitionMapLock.Unlock()
		p.deleteBrokerConn(&partition.Addr)
		var tmp []*protocol.Partition
		for _, par := range p.partitionMap[topic] { //删除所有失效分区
			if par.Addr != partition.Addr {
				tmp = append(tmp, par)
			}
		}
		p.partitionMap[topic] = tmp
		p.changeConsistentHashMap(p.partitionMap[topic], topic)
	}
	return err
}

//发布消息到，不等待ack
func (p *Producer) PubilshWithoutAck(topic string, data []byte, prioroty int32) error {
	msg := &protocol.Message{
		Priority: prioroty,
		Msg:      data,
	}
	partition, err := p.getRandomPartition(topic)
	if err != nil {
		return err
	}
	err = p.pubilshWithoutAckInternal(partition, msg)
	if err != nil { //该分区所在已经失效，删除
		p.partitionMapLock.Lock()
		defer p.partitionMapLock.Unlock()
		p.deleteBrokerConn(&partition.Addr)
		var tmp []*protocol.Partition
		for _, par := range p.partitionMap[topic] { //删除所有失效分区
			if par.Addr != partition.Addr {
				tmp = append(tmp, par)
			}
		}
		p.partitionMap[topic] = tmp
	}
	return err
}

//发布消息到，不等待ack内部实现
func (p *Producer) pubilshWithoutAckInternal(partition *protocol.Partition, msg *protocol.Message) error {

	if partition == nil {
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

	requestData := &protocol.PublishReq{
		PartitionName: partition.Name,
		Msg:           msg,
	}
	data, err := proto.Marshal(requestData)
	if err != nil {
		myLogger.Logger.PrintError("marshaling error", err)
		return err
	}
	reqData, err := protocalFuc.PackClientServerProtoBuf(protocol.ClientServerCmd_CmdPublishWithoutAskReq, data)
	if err != nil {
		myLogger.Logger.PrintError("marshaling error", err)
		return err
	}

	err = brokerConn.WriteDefer(reqData)
	if err != nil {
		myLogger.Logger.PrintError("controllerConn Write err", err)
		return err
	}
	myLogger.Logger.Printf("Pubilsh %s", requestData)
	return nil
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
