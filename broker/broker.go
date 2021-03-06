package broker

import (
	"../mylib/etcdClient"
	"../mylib/myLogger"
	"../mylib/protocalFuc"
	"../protocol"
	"flag"
	"fmt"
	"github.com/golang/protobuf/proto"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

const logDir string = "./broker"
const openCluster bool = true
const defaultClientListenAddr = "0.0.0.0:12345" //client监听地址
const defaultBrokerListenAddr = "0.0.0.0:12346" //broker监听地址
const defaultEtcdAddr = "9.135.8.253:2379"      //etcd服务器地址
const defaultQueueSize = 1024 * 1024 * 10       //队列大小
type Broker struct {
	isController bool
	Id           string

	maxClientId int64

	maddr           *protocol.ListenAddr //我的监听地址
	controllerAddr  string               //master的监听地址
	clientTcpServer *clientTcpServer     //client
	brokerTcpServer *brokerTcpServer     //controller

	topicMapLock sync.RWMutex
	topicMap     map[string]*topic

	brokerMapLock  sync.RWMutex
	aliveBrokerMap map[string]*controller2BrokerConn //保存连接到controller的broker，key为brokerAddr

	partitionMapLock sync.RWMutex
	partitionMap     map[string]*partition //partionName to *partition

	groupMapLock sync.RWMutex
	groupMap     map[string]*group

	//clientMapLock sync.RWMutex
	clientMap map[int64]*client

	tcpListener net.Listener
	wg          sync.WaitGroup

	clientChangeChan chan *clientChange

	exitSignal chan os.Signal
	//exitChan chan string

	needExit bool

	etcdClient *etcdClient.EtcdClient

	broker2ControllerConn *broker2ControllerConn

	queueSize int
}
type clientChange struct {
	isAdd        bool
	client       *client
	waitFinished chan bool
}

func New(exitSignal chan os.Signal) (*Broker, error) {
	clientListenAddr := flag.String("clientListenAddr", defaultClientListenAddr, "ip:port") //客户端服务器监听地址
	brokerListenAddr := flag.String("brokerListenAddr", defaultBrokerListenAddr, "ip:port") //broker服务器监听地址
	etcdAddr := flag.String("etcdAddr", defaultEtcdAddr, "ip:port")                         //etcd地址
	queueSize := flag.Int("queueSize", defaultQueueSize, "bytes")
	flag.Parse() //解析参数

	//转换为本地ip
	host, port, _ := net.SplitHostPort(*clientListenAddr)
	if host == "0.0.0.0" {
		*clientListenAddr = getIntranetIp() + ":" + port
		//fmt.Println(*clientListenAddr)
	}
	Id := port
	host, port, _ = net.SplitHostPort(*brokerListenAddr)
	if host == "0.0.0.0" {
		_, port, _ := net.SplitHostPort(*brokerListenAddr)
		*brokerListenAddr = getIntranetIp() + ":" + port
		//fmt.Println(*brokerListenAddr)
	}

	broker := &Broker{
		Id:               Id,
		topicMap:         make(map[string]*topic),
		groupMap:         make(map[string]*group),
		clientMap:        make(map[int64]*client),
		partitionMap:     make(map[string]*partition),
		aliveBrokerMap:   make(map[string]*controller2BrokerConn),
		clientChangeChan: make(chan *clientChange),
		maddr: &protocol.ListenAddr{
			ClientListenAddr: *clientListenAddr,
			BrokerListenAddr: *brokerListenAddr,
		},
		exitSignal: exitSignal,
		queueSize:  *queueSize,
	}

	_, err := myLogger.New(logDir + broker.Id + "log/")
	myLogger.Logger.Print(*clientListenAddr, " ", *brokerListenAddr)

	tcpServer := newClientTcpServer(broker, *clientListenAddr)
	broker.clientTcpServer = tcpServer
	if openCluster { //开启集群时需要连接到controller，或者竞选成为controller
		broker.etcdClient, err = etcdClient.NewEtcdClient(broker, etcdAddr) //连接到etcd
		broker.etcdClient.ClearMetaData()
		time.Sleep(1 * time.Second) //等待一会，看能竞选成为controller
		if !broker.isController {   //没有竞选成为controller，需要连接到controller
			masterAddr, err := broker.etcdClient.GetControllerAddr() //读取controller的地址
			if err != nil {
				myLogger.Logger.PrintError(err)
			} else {
				if masterAddr.BrokerListenAddr != broker.maddr.BrokerListenAddr { //当前我不是master,否则继续等待竞选结果
					broker.controllerAddr = masterAddr.BrokerListenAddr
					broker.connect2Controller(broker.controllerAddr)
					myLogger.Logger.PrintDebug("竞选失败，成为普通broker")
				}
			}

		}
	} else {
		//broker.connect2Controller() //不需要连接controller
	}
	return broker, err

}

//获取本机Ip
func getIntranetIp() string {
	addrs, err := net.InterfaceAddrs()

	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	for _, address := range addrs {
		if ipnet, ok := address.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
			if ipnet.IP.To4() != nil {
				return ipnet.IP.String()
			}
		}
	}
	return "0.0.0.0"
}

//恢复集群数据
func (b *Broker) retrieveMetaData() {
	metaData := &protocol.MetaData{}
	data, err := b.etcdClient.GetMetaData()
	if err != nil {
		myLogger.Logger.PrintError("GetMetaData error", err)
		return
	}
	err = proto.Unmarshal(data, metaData)
	if err != nil {
		myLogger.Logger.PrintError("Unmarshal error", err)
		return
	}
	myLogger.Logger.Print("retrieveMetaData:", metaData)
	b.topicMap = make(map[string]*topic) //创建topicMap
	for _, t := range metaData.Topics {
		topic := newTopic(t.Name, b)
		b.addTopic(&t.Name, topic)
		for _, p := range t.Partitions {
			b.partitionMapLock.Lock()
			partition, ok := b.partitionMap[p.Name]
			if !ok { //该分区不存在，新建
				partition = newPartition(p.Name, p.Addr, p.Addr == b.maddr.ClientListenAddr, b, t.Name)
				b.partitionMap[p.Name] = partition
			}
			topic.AddPartition(partition) //保存分区到topic
			b.partitionMapLock.Unlock()
		}
	}

	b.groupMap = make(map[string]*group) //创建
	for _, g := range metaData.Groups {
		group := newGroup(g.Name, g.RebalanceID, b)
		b.addGroup(group)
		for _, topicName := range g.SubscribedTopics {
			t, _ := b.getTopic(&topicName)
			group.addTopic(t)

			t.partitionMapLock.Lock()
			for _, p := range t.partitionMap {
				if p.isNativePartition {
					p.addComsummerGroup(group.name) //添加消费者组
				}
			}
			t.partitionMapLock.Unlock()
		}
	}
}

//controller发生变化时的回调函数
func (b *Broker) ChangeControllerAddr(addr *protocol.ListenAddr) { //master地址变化时被调用
	b.controllerAddr = addr.BrokerListenAddr
	if !b.isController { //我不是controller
		if b.broker2ControllerConn != nil {
			if b.broker2ControllerConn.addr != b.controllerAddr { //controller以更换
				myLogger.Logger.Print("Change broker2ControllerConn ", b.controllerAddr)
				b.broker2ControllerConn.Close()        //关闭原来的
				b.connect2Controller(b.controllerAddr) //连接新的
			} else {
				myLogger.Logger.Print("the same broker2ControllerConn ", b.controllerAddr)
			}
		} else {
			b.connect2Controller(b.controllerAddr)
		}
	}
	myLogger.Logger.Print("ChangeControllerAddr ", addr)
}

//竞选成为controller时的回调函数
func (b *Broker) BecameController() {
	b.isController = true
	b.retrieveMetaData()                                                //取回集群数据
	b.brokerTcpServer = NewBrokerTcpServer(b, b.maddr.BrokerListenAddr) //监听broker连接
	go b.brokerTcpServer.startTcpServer()                               //开启broker监听服务
	if b.broker2ControllerConn != nil {
		myLogger.Logger.Print("close broker2ControllerConn")
		b.broker2ControllerConn.Close() //关闭到master的连接
		b.broker2ControllerConn = nil
	}
	if err := b.etcdClient.PutControllerAddr(b.maddr); err != nil { //上传自己的监听地址
		myLogger.Logger.PrintError(err)
		return
	}
	myLogger.Logger.PrintDebug("竞选成功，成为Controller")
	myLogger.Logger.Print("BecameController")
}

//被迫放弃controller时的回调函数
func (b *Broker) BecameNormalBroker() { //被取消controller资格时调用，断线超过15秒没有发心跳就会失去leader资格
	b.isController = false
	if b.brokerTcpServer != nil {
		b.brokerTcpServer.Close()
	}
	if b.broker2ControllerConn != nil {
		if b.broker2ControllerConn.addr != b.controllerAddr { //controller以更换
			b.broker2ControllerConn.Close()        //关闭原来的
			b.connect2Controller(b.controllerAddr) //连接新的
		}
	} else {
		b.connect2Controller(b.controllerAddr)
	}
	b.CloseAllBrokerConn()
	myLogger.Logger.Print("BecameNormalBroker")
}

//保存集群元数据
func (b *Broker) persistMetaData() {
	metaData := &protocol.MetaData{}
	for topicName, topic := range b.topicMap {
		t := &protocol.Topic{
			Name:       topicName,
			Partitions: topic.getPartitions(),
		}
		metaData.Topics = append(metaData.Topics, t)
	}
	for groupName, group := range b.groupMap {
		g := &protocol.Group{
			Name:             groupName,
			SubscribedTopics: group.getSubscribedTopics(),
			RebalanceID:      group.rebalanceID,
		}
		metaData.Groups = append(metaData.Groups, g)
	}
	data, err := proto.Marshal(metaData)
	if err != nil {
		myLogger.Logger.PrintError("marshaling error: ", err)
		return
	}
	myLogger.Logger.Print(metaData)
	err = b.etcdClient.PutMetaData(string(data))
	if err != nil {
		myLogger.Logger.PrintError("marshaling error: ", err)
		return
	}
	return
}

//启动broker
func (b *Broker) Run() error {
	//监听指定退出信号 ctrl+c kill
	//signal.Notify(b.exitSignal, os.Interrupt, os.Kill)
	//signal.Notify(b.exitSignal)
	b.wg.Add(2)
	go func() {
		b.ReadLoop()
		b.wg.Done()
	}()
	go func() {
		b.clientTcpServer.startTcpServer()
		b.wg.Done()
	}()

	b.wg.Wait() //等待上面的两个协程关闭

	myLogger.Logger.Print("Broker Run exit ")
	return nil
}

//获取topic
func (b *Broker) getTopic(topicName *string) (*topic, bool) {
	b.topicMapLock.RLock()
	topic, ok := b.topicMap[*topicName]
	b.topicMapLock.RUnlock()
	return topic, ok
}

//添加topic
func (b *Broker) addTopic(topicName *string, topic *topic) {
	b.topicMapLock.Lock()
	b.topicMap[*topicName] = topic
	b.topicMapLock.Unlock()
	myLogger.Logger.Print("addTopic success, remain len:", len(b.topicMap))
	return
}

//删除topic
func (b *Broker) deleteTopic(topicName *string) {
	b.topicMapLock.Lock()
	defer b.topicMapLock.Unlock()

	_, ok := b.topicMap[*topicName]
	if !ok {
		myLogger.Logger.Print("deleteTopic not exist, remain len:", len(b.topicMap))
		return
	}
	delete(b.topicMap, *topicName)
	myLogger.Logger.Print("deleteTopic success, remain len:", len(b.topicMap))
	return
}

//获取分区,由调用方加锁
func (b *Broker) getPartition(partitionName *string) (*partition, bool) {
	//b.partitionMapLock.RLock()
	partition, ok := b.partitionMap[*partitionName]
	//b.partitionMapLock.RUnlock()
	return partition, ok
}

//获取分区并加读锁
func (b *Broker) getPartitionAndLock(partitionName *string) (*partition, bool) {
	b.partitionMapLock.RLock()
	partition, ok := b.partitionMap[*partitionName]
	return partition, ok
}

//解锁，配合上面使用
func (b *Broker) getPartitionUnlock() {
	b.partitionMapLock.RUnlock()
}

//
//func (b *Broker)GetAllPartitions() []*protocol.Partition {
//	var partitions []*protocol.Partition
//	b.partitionMapLock.RLock()
//	for _, par := range b.partitionMap{
//		tmp := &protocol.Partition{
//			Name: par.name,
//			Addr: par.addr,
//		}
//		partitions = append(partitions, tmp)
//	}
//	b.partitionMapLock.RUnlock()
//	return partitions
//}

//获取某个broker的分区并将其添加到对应topic
func (b *Broker) GetAndAddPartitionForBroker(addr *string) ([]*protocol.Partition, *map[string]bool) {
	if !b.isController {
		myLogger.Logger.PrintWarning("try to deletePartitionForBroker int normal Broker")
		return nil, nil
	}
	var partitions []*protocol.Partition
	var dirtyTopics = make(map[string]bool)
	b.partitionMapLock.RLock()
	for _, par := range b.partitionMap {
		if par.addr == *addr { //broker上的地址
			tmp := &protocol.Partition{
				Name: par.name,
				Addr: par.addr,
			}
			partitions = append(partitions, tmp)
			topic, _ := b.getTopic(&par.belongtopic)
			topic.AddPartition(par)        //从topic上添加
			dirtyTopics[topic.name] = true //记录修改过的topic
		}
	}
	b.partitionMapLock.RUnlock()
	return partitions, &dirtyTopics
}

//获取某个broker的分区并将其从对应topic删除
func (b *Broker) GetAndDeletePartitionForBroker(addr *string) {
	if !b.isController {
		myLogger.Logger.PrintWarning("try to deletePartitionForBroker int normal Broker")
		return
	}
	myLogger.Logger.Print("deletePartitionForBroker")
	var dirtyTopics = make(map[string]bool)
	b.partitionMapLock.RLock()
	for _, par := range b.partitionMap {
		if par.addr == *addr { //broker上的地址，需删除
			topic, _ := b.getTopic(&par.belongtopic)
			topic.deletePartition(&par.name) //从topic上删除该分区
			dirtyTopics[topic.name] = true   //记录修改过的topic
		}
	}
	b.partitionMapLock.RUnlock()
	if openCluster {
		for topicName, _ := range dirtyTopics { //修改过的分区需上传的etcd
			topic, _ := b.getTopic(&topicName)
			pars := &protocol.Partitions{ //创建分区的消息
				Partition: topic.getPartitions(),
			}
			data, err := proto.Marshal(pars)
			if err != nil {
				myLogger.Logger.PrintError("marshaling error: ", err)
				continue
			}
			myLogger.Logger.Print(topicName, " Put Patitions : ", pars)
			b.etcdClient.PutPatitions(topicName, string(data)) //将分区改变保存到集群
		}
	}
	return
}

//添加分区
func (b *Broker) addPartition(partitionName *string, partition *partition) {
	b.partitionMapLock.Lock()
	b.partitionMap[*partitionName] = partition
	b.partitionMapLock.Unlock()
	return
}

//删除分区,由调用方加锁
func (b *Broker) deletePartition(partitionName *string) {
	//b.partitionMapLock.Lock()
	//defer b.partitionMapLock.Unlock()

	_, ok := b.partitionMap[*partitionName]
	if !ok {
		myLogger.Logger.Print("deletePartition not exist, remain len:", len(b.clientMap))
		return
	}
	delete(b.partitionMap, *partitionName)
	myLogger.Logger.Print("deletePartition success, remain len:", len(b.clientMap))
	return
}

//删除分区
func (b *Broker) deletePartitionAutoLock(partitionName *string) {
	b.partitionMapLock.Lock()
	defer b.partitionMapLock.Unlock()

	_, ok := b.partitionMap[*partitionName]
	if !ok {
		myLogger.Logger.Print("deletePartition not exist, remain len:", len(b.partitionMap))
		return
	}
	delete(b.partitionMap, *partitionName)
	myLogger.Logger.Print("deletePartition success, remain len:", len(b.partitionMap))
	return
}

//获取消费者组对象
func (b *Broker) getGroup(groupName *string) (*group, bool) {
	b.groupMapLock.RLock()
	group, ok := b.groupMap[*groupName]
	b.groupMapLock.RUnlock()
	return group, ok
}

//添加消费者组
func (b *Broker) addGroup(group *group) {
	b.groupMapLock.Lock()
	b.groupMap[group.name] = group
	b.groupMapLock.Unlock()
	return
}

//重均衡所有消费者组
func (b *Broker) RebanlenceAllGroup() {
	b.groupMapLock.Lock()
	for _, group := range b.groupMap {
		group.Rebalance()
	}
	b.groupMapLock.Unlock()
}

//获取client实例
func (b *Broker) getClient(clientId int64) (*client, bool) {
	//b.clientMapLock.RLock()
	client, ok := b.clientMap[clientId]
	//b.clientMapLock.RUnlock()
	return client, ok
}

//添加client实例
func (b *Broker) addClient(client *client) {
	//b.clientMapLock.Lock()
	b.clientMap[client.id] = client
	myLogger.Logger.Print("addClient, pre len:", len(b.clientMap))
	//b.clientMapLock.Unlock()
	return
}

//删除client实例
func (b *Broker) removeClient(clientId int64) {
	//b.clientMapLock.Lock()
	_, ok := b.clientMap[clientId]
	if !ok {
		myLogger.Logger.Print("remove broker Client not exist, remain len:", len(b.clientMap))
		//b.clientMapLock.Unlock()
		return
	}
	delete(b.clientMap, clientId)
	myLogger.Logger.Print("remove broker Client success, remain len:", len(b.clientMap))
	//b.clientMapLock.Unlock()
}

//关闭client连接
func (b *Broker) closeClients() {
	if b.clientTcpServer.listener != nil {
		b.clientTcpServer.listener.Close() //关闭tcp监听
	}

	for _, client := range b.clientMap {
		//client.isbrokerExitLock1.Lock()
		client.isbrokerExit = true //标志退出了
		//client.isbrokerExitLock1.Unlock()
		err := client.conn.Close() //关闭所有conn，从而关闭所有client协程
		if err != nil {
			myLogger.Logger.PrintError("client close err:", err)
		}
	}
}

//退出broker
func (b *Broker) exit() {
	b.partitionMapLock.RLock()
	for _, partition := range b.partitionMap {
		partition.exit()
	}
	b.partitionMapLock.RUnlock()

	if openCluster && b.isController {
		b.persistMetaData()
	}
}

//获取broker连接
func (b *Broker) GetBrokerConn(brokerAddr *string) (*controller2BrokerConn, bool) {
	b.brokerMapLock.RLock()
	brokerConn, ok := b.aliveBrokerMap[*brokerAddr]
	b.brokerMapLock.RUnlock()
	return brokerConn, ok
}

//关闭所有broker连接
func (b *Broker) CloseAllBrokerConn() {
	myLogger.Logger.Print("CloseAllBrokerConn: ")
	b.brokerMapLock.Lock()
	defer b.brokerMapLock.Unlock()

	for _, controller2BrokerConn := range b.aliveBrokerMap {
		controller2BrokerConn.conn.Close()
		delete(b.aliveBrokerMap, controller2BrokerConn.clientListenAddr)
	}
}

//删除broker连接
func (b *Broker) DeleteBrokerConn(controller2BrokerConn *controller2BrokerConn) bool {
	b.brokerMapLock.Lock()
	defer b.brokerMapLock.Unlock()
	_, ok := b.aliveBrokerMap[controller2BrokerConn.clientListenAddr]
	if !ok {
		return false
	}
	delete(b.aliveBrokerMap, controller2BrokerConn.clientListenAddr)
	myLogger.Logger.Print("DeleteBrokerConn: ", controller2BrokerConn.clientListenAddr, " len: ", len(b.aliveBrokerMap))
	return true
}

//添加broker连接
func (b *Broker) AddBrokerConn(controller2BrokerConn *controller2BrokerConn) bool {
	b.brokerMapLock.Lock()
	defer b.brokerMapLock.Unlock()
	_, ok := b.aliveBrokerMap[controller2BrokerConn.clientListenAddr]
	if ok {
		return false
	}
	b.aliveBrokerMap[controller2BrokerConn.clientListenAddr] = controller2BrokerConn
	myLogger.Logger.Print("AddBrokerConn: ", controller2BrokerConn.clientListenAddr, " len: ", len(b.aliveBrokerMap))
	return true
}

//判断给broker是否掉线
func (b *Broker) IsbrokerAlive(brokerAddr *string) bool {
	if *brokerAddr == b.maddr.ClientListenAddr { //该broker地址是本机，有效
		return true
	}
	b.brokerMapLock.RLock()
	defer b.brokerMapLock.RUnlock()
	_, ok := b.aliveBrokerMap[*brokerAddr] //该broker地址是否存活
	return ok
}

//处理退出信号及管理client
func (b *Broker) ReadLoop() {
	for {
		myLogger.Logger.Print("Broker readLoop")
		select {
		case s := <-b.exitSignal: //退出信号来了
			myLogger.Logger.Print("exitSignal:", s)
			myLogger.Logger.PrintDebug("exitSignal:", s)
			b.needExit = true //标志要退出了
			b.closeClients()  //关闭所有client
			//goto exit
			if b.needExit && len(b.clientMap) == 0 { //没有client，直接退出了,要等所有client退出我才能退出readLoop，否则会出bug
				b.exit()
				goto exit
			}
		case tmp := <-b.clientChangeChan: //所有对clientMap的操作
			myLogger.Logger.Print("here")
			if tmp.isAdd {
				b.addClient(tmp.client)
			} else {
				b.removeClient(tmp.client.id)
				if b.needExit && len(b.clientMap) == 0 { //删完client了，该退出了
					tmp.waitFinished <- true
					b.exit()
					goto exit
				}
			}
			tmp.waitFinished <- true
		}
	}
exit:
	myLogger.Logger.Print("exit broker ReadLoop")
}

//删除topic及其分区
func (b *Broker) DeleteTopic(topicName string) (response []byte) {
	if !b.isController {
		myLogger.Logger.PrintWarning("try to deleteTopic int normal Broker")
		return nil
	}
	topic, ok := b.getTopic(&topicName)
	if !ok {
		myLogger.Logger.Printf("try to delete not existed topic : %s", topicName)

		rsp := &protocol.DeleteTopicRsp{
			Ret: protocol.RetStatus_Fail,
		}
		data, err := proto.Marshal(rsp)
		if err != nil {
			myLogger.Logger.PrintError("marshaling error", err)
			return nil
		}
		reqData, err := protocalFuc.PackClientServerProtoBuf(protocol.ClientServerCmd_CmdDeleteTopicRsp, data)
		if err != nil {
			myLogger.Logger.PrintError("marshaling error", err)
			return nil
		}
		myLogger.Logger.Printf("write: %s", rsp)
		response = reqData

	} else {
		topic.deleteAllPartitions()

		rsp := &protocol.DeleteTopicRsp{
			Ret: protocol.RetStatus_Successs,
		}
		data, err := proto.Marshal(rsp)
		if err != nil {
			myLogger.Logger.PrintError("marshaling error", err)
			return nil
		}
		reqData, err := protocalFuc.PackClientServerProtoBuf(protocol.ClientServerCmd_CmdDeleteTopicRsp, data)
		if err != nil {
			myLogger.Logger.PrintError("marshaling error", err)
			return nil
		}
		myLogger.Logger.Printf("write: %s", rsp)
		response = reqData
		b.deleteTopic(&topicName)
	}
	return response
}

//创建topic
func (b *Broker) CreatTopic(topicName string, partitionNum int32) (response []byte) {
	if !b.isController {
		myLogger.Logger.PrintWarning("try to creatTopic in normal Broker")
		return nil
	}
	topic, ok := b.getTopic(&topicName)
	if ok {
		myLogger.Logger.Printf("try to create existed topic : %s %d", topicName, int(partitionNum))
		rsp := &protocol.CreatTopicRsp{
			Ret: protocol.RetStatus_Fail,
		}
		data, err := proto.Marshal(rsp)
		if err != nil {
			myLogger.Logger.PrintError("marshaling error", err)
			return nil
		}
		reqData, err := protocalFuc.PackClientServerProtoBuf(protocol.ClientServerCmd_CmdCreatTopicRsp, data)
		if err != nil {
			myLogger.Logger.PrintError("marshaling error", err)
			return nil
		}
		myLogger.Logger.Printf("write: %s", rsp)
		response = reqData

	} else {
		myLogger.Logger.Printf("create topic : %s %d", topicName, int(partitionNum))
		topic = newTopic(topicName, b)
		var addrs []string
		b.brokerMapLock.RLock()
		addrs = append(addrs, b.maddr.ClientListenAddr) //添加本机地址
		for addr, _ := range b.aliveBrokerMap {         //添加所有存活的地址
			addrs = append(addrs, addr)
		}
		b.brokerMapLock.RUnlock()
		topic.CreatePartitions(int(partitionNum), addrs) //创建partitionNum个分区
		b.addTopic(&topicName, topic)
		rsp := &protocol.CreatTopicRsp{
			Ret:        protocol.RetStatus_Successs,
			Partitions: topic.getPartitions(),
		}
		data, err := proto.Marshal(rsp)
		if err != nil {
			myLogger.Logger.PrintError("marshaling error", err)
			return nil
		}
		reqData, err := protocalFuc.PackClientServerProtoBuf(protocol.ClientServerCmd_CmdCreatTopicRsp, data)
		if err != nil {
			myLogger.Logger.PrintError("marshaling error", err)
			return nil
		}
		myLogger.Logger.Printf("write: %s", rsp)
		response = reqData

	}
	return response
}

//获取生产者的分区
func (b *Broker) GetPublisherPartition(topicName string) (response []byte) {
	if !b.isController {
		myLogger.Logger.PrintWarning("try to getPublisherPartition int normal Broker")
		return nil
	}
	topic, ok := b.getTopic(&topicName)
	if ok {
		myLogger.Logger.Printf("getPublisherPartition : %s", topicName)

		rsp := &protocol.GetPublisherPartitionRsp{
			Ret:        protocol.RetStatus_Successs,
			Partitions: topic.getPartitions(),
		}
		data, err := proto.Marshal(rsp)
		if err != nil {
			myLogger.Logger.PrintError("marshaling error", err)
			return nil
		}
		reqData, err := protocalFuc.PackClientServerProtoBuf(protocol.ClientServerCmd_CmdGetPublisherPartitionRsp, data)
		if err != nil {
			myLogger.Logger.PrintError("marshaling error", err)
			return nil
		}
		myLogger.Logger.Printf("write: %s", rsp)
		response = reqData

	} else {
		myLogger.Logger.Printf("Partition Not existed : %s", topicName)

		rsp := &protocol.GetPublisherPartitionRsp{
			Ret: protocol.RetStatus_Fail,
		}
		data, err := proto.Marshal(rsp)
		if err != nil {
			myLogger.Logger.PrintError("marshaling error", err)
			return nil
		}
		reqData, err := protocalFuc.PackClientServerProtoBuf(protocol.ClientServerCmd_CmdGetPublisherPartitionRsp, data)
		if err != nil {
			myLogger.Logger.PrintError("marshaling error", err)
			return nil
		}
		myLogger.Logger.Printf("write: %s", rsp)
		response = reqData
	}
	return response
}

//获取消费者的分区
func (b *Broker) GetConsumerPartition(groupName string, clientID int64) (response []byte) {
	if !b.isController {
		myLogger.Logger.PrintWarning("try to getConsumerPartition int normal Broker")
		return nil
	}
	group, ok := b.getGroup(&groupName)
	myLogger.Logger.Printf("registerComsummer : %s", groupName)
	if !ok {
		return nil
		//group = newGroup(groupName)
		//b.addGroup(group)
	}

	partitions := group.getClientPartition(clientID)
	if partitions == nil { //不存在
		return nil
	}

	rsp := &protocol.GetConsumerPartitionRsp{
		Ret:        protocol.RetStatus_Successs,
		Partitions: partitions,
	}
	data, err := proto.Marshal(rsp)
	if err != nil {
		myLogger.Logger.PrintError("marshaling error", err)
		return nil
	}
	reqData, err := protocalFuc.PackClientServerProtoBuf(protocol.ClientServerCmd_CmdGetConsumerPartitionRsp, data)
	if err != nil {
		myLogger.Logger.PrintError("marshaling error", err)
		return nil
	}
	myLogger.Logger.Printf("write: %s", rsp)
	response = reqData
	return response
}

//消费者订阅topic
func (b *Broker) SubscribeTopic(topicName string, groupName string, clientID int64) (response []byte) {
	if !b.isController {
		myLogger.Logger.PrintWarning("try to subscribeTopic int normal Broker")
		return nil
	}
	topic, ok := b.getTopic(&topicName)
	if !ok {
		myLogger.Logger.Printf("Topic Not existed : %s", topicName, len(b.topicMap))
		rsp := &protocol.SubscribePartitionRsp{
			Ret: protocol.RetStatus_Fail,
		}
		data, err := proto.Marshal(rsp)
		if err != nil {
			myLogger.Logger.PrintError("marshaling error", err)
			return nil
		}
		reqData, err := protocalFuc.PackClientServerProtoBuf(protocol.ClientServerCmd_CmdSubscribePartitionRsp, data)
		if err != nil {
			myLogger.Logger.PrintError("marshaling error", err)
			return nil
		}
		myLogger.Logger.Printf("write: %s", rsp)
		response = reqData
		return response
	}

	group, ok := b.getGroup(&groupName)
	myLogger.Logger.Printf("registerComsummer : %s", groupName)
	if !ok {
		myLogger.Logger.Printf("newGroup : %s", groupName)
		group = newGroup(groupName, 0, b)
		b.addGroup(group)
	}
	clientConn, ok := b.getClient(clientID)
	if !ok {
		myLogger.Logger.Print("clientConn have close")
	}

	clientConn.belongGroup = groupName
	succ := group.addTopic(topic)
	succ = group.addClient(clientConn) || succ
	if succ {
		group.Rebalance()
		return nil
	} else {
		group.Rebalance()
		return nil
	}
}

//消费者订阅指定分区
func (b *Broker) SubscribePartition(partitionName string, groupName string, clientID int64, RebalanceId int32) (response []byte) {
	//b.partitionMapLock.RLock()
	//defer b.partitionMapLock.RUnlock()
	//partition, ok := b.getPartition(&partitionName)
	partition, ok := b.getPartitionAndLock(&partitionName)
	defer b.getPartitionUnlock()
	if !ok {
		myLogger.Logger.Printf("Partition Not existed : %s", partitionName)

		rsp := &protocol.SubscribePartitionRsp{
			Ret: protocol.RetStatus_Fail,
		}
		data, err := proto.Marshal(rsp)
		if err != nil {
			myLogger.Logger.PrintError("marshaling error", err)
			return nil
		}
		reqData, err := protocalFuc.PackClientServerProtoBuf(protocol.ClientServerCmd_CmdSubscribePartitionRsp, data)
		if err != nil {
			myLogger.Logger.PrintError("marshaling error", err)
			return nil
		}
		myLogger.Logger.Printf("write: %s", rsp)
		response = reqData
	} else {
		if !partition.isNativePartition {
			myLogger.Logger.PrintError("subscribePartition is not native:", partition.name, partition.addr)
			rsp := &protocol.SubscribePartitionRsp{
				Ret: protocol.RetStatus_Fail,
			}
			data, err := proto.Marshal(rsp)
			if err != nil {
				myLogger.Logger.PrintError("marshaling error", err)
				return nil
			}
			reqData, err := protocalFuc.PackClientServerProtoBuf(protocol.ClientServerCmd_CmdSubscribePartitionRsp, data)
			if err != nil {
				myLogger.Logger.PrintError("marshaling error", err)
				return nil
			}
			myLogger.Logger.Printf("write: %s", rsp)
			response = reqData
			return response
		}
		if partition.getGroupRebalanceId(groupName) > RebalanceId { //不是最新的，丢弃
			myLogger.Logger.Printf("Reject subscribePartition oldId : %d newId: %d", partition.getGroupRebalanceId(groupName), RebalanceId)
			return nil
		}
		clientConn, ok := b.getClient(clientID)
		if !ok {
			myLogger.Logger.Print("clientConn have close")
			return nil
		}
		clientConn.consumePartions[partitionName] = true
		partition.addComsummerClient(clientConn, groupName, RebalanceId)

		rsp := &protocol.SubscribePartitionRsp{
			Ret: protocol.RetStatus_Successs,
		}
		data, err := proto.Marshal(rsp)
		if err != nil {
			myLogger.Logger.PrintError("marshaling error", err)
			return nil
		}
		reqData, err := protocalFuc.PackClientServerProtoBuf(protocol.ClientServerCmd_CmdSubscribePartitionRsp, data)
		if err != nil {
			myLogger.Logger.PrintError("marshaling error", err)
			return nil
		}
		myLogger.Logger.Printf("write: %s", rsp)
		response = reqData
		clientConn.belongGroup = groupName
	}
	return response
}

//注册消费者
func (b *Broker) RegisterConsumer(groupName string, clientID int64) (response []byte) {
	//request := data.client2serverData
	//groupName := request.GroupName
	group, ok := b.getGroup(&groupName)
	myLogger.Logger.Printf("registerComsummer : %s", groupName)
	if !ok {
		group = newGroup(groupName, 0, b)
		b.addGroup(group)
	}
	clientConn, ok := b.getClient(clientID)
	if !ok {
		myLogger.Logger.Print("clientConn have close")
	}
	succ := group.addClient(clientConn)
	if succ { //已通过go balance response
		return nil
	}

	rsp := &protocol.RegisterConsumerRsp{
		Ret: protocol.RetStatus_Successs,
	}
	data, err := proto.Marshal(rsp)
	if err != nil {
		myLogger.Logger.PrintError("marshaling error", err)
		return nil
	}
	reqData, err := protocalFuc.PackClientServerProtoBuf(protocol.ClientServerCmd_CmdRegisterConsumerRsp, data)
	if err != nil {
		myLogger.Logger.PrintError("marshaling error", err)
		return nil
	}
	myLogger.Logger.Printf("write: %s", rsp)
	response = reqData
	return response
}

//取消注册
func (b *Broker) UnRegisterConsumer(groupName string, clientID int64) (response []byte) {
	group, ok := b.getGroup(&groupName)
	myLogger.Logger.Printf("registerComsummer : %s", groupName)
	if !ok {
		myLogger.Logger.Printf("group not exist : %s", groupName)
		return nil
	}
	group.deleteClient(clientID)
	return nil
}

//生成客户端Id
func (b *Broker) GenerateClientId() int64 {
	return atomic.AddInt64(&b.maxClientId, 1)
}

//连接到controller
func (b *Broker) connect2Controller(addr string) error {
	var err error
	b.broker2ControllerConn, err = NewBroker2ControllerConn(addr, b)
	if err != nil {
		myLogger.Logger.PrintfError(" connecting to Controller error: %s %s", addr, err)
		return err
	}
	myLogger.Logger.Printf("connecting to Controller - %s", addr)
	broker2ControllerData := &protocol.Broker2Controller{
		Key:  protocol.Broker2ControllerKey_RegisterBroker,
		Addr: b.maddr,
	}
	b.broker2ControllerConn.Put(broker2ControllerData)
	return nil
}

//
//func (b *Broker)  creatTopic(data *readData)  (response *protocol.Server2Client) {
//	if !b.isController{
//		myLogger.Logger.PrintWarning("try to creatTopic int normal Broker")
//		return nil
//	}
//	request := data.client2serverData
//	topicName := request.Topic
//	partitionNum := request.PartitionNum
//	topic, ok := b.getTopic(&topicName)
//	if ok {
//		myLogger.Logger.Printf("try to create existed topic : %s %d", topicName, int(partitionNum))
//		response = &protocol.Server2Client{
//			Key: protocol.Server2ClientKey_TopicExisted,
//			//Partitions: topic.getPartitions(),
//		}
//	} else {
//		myLogger.Logger.Printf("create topic : %s %d", topicName, int(partitionNum))
//		topic = newTopic(topicName, b)
//		var addrs []string
//		b.brokerMapLock.RLock()
//		addrs = append(addrs, b.maddr.ClientListenAddr) //添加本机地址
//		for addr, _ := range b.aliveBrokerMap{          //添加所有存活的地址
//			addrs = append(addrs, addr)
//		}
//		b.brokerMapLock.RUnlock()
//		topic.CreatePartitions(int(partitionNum), addrs) //创建partitionNum个分区
//		b.addTopic(&topicName, topic)
//		response = &protocol.Server2Client{
//			Key: protocol.Server2ClientKey_SendPartions,
//			Topic: topicName,
//			Partitions: topic.getPartitions(),
//		}
//
//		//b.groupMapLock.RLock()
//		//defer b.groupMapLock.RUnlock()
//		//for _, group := range b.groupMap{
//		//	topics := group.getSubscribedTopics()
//		//	for _, t := range topics{
//		//		if t == topic.name{//检查是否有消费者以订阅该topic
//		//			topic.partitionMapLock.RLock()
//		//			for _, p := range topic.partitionMap{
//		//				p.addComsummerGroup(group.name) //添加消费者组
//		//			}
//		//			topic.partitionMapLock.RUnlock()
//		//		}
//		//	}
//		//}
//	}
//	return response
//}

//func (b *Broker)  getPublisherPartition(data *readData)   (response *protocol.Server2Client) {
//	if !b.isController{
//		myLogger.Logger.PrintWarning("try to getPublisherPartition int normal Broker")
//		return nil
//	}
//	request := data.client2serverData
//	topicName := request.Topic
//	topic, ok := b.getTopic(&topicName)
//	if ok {
//		myLogger.Logger.Printf("getPublisherPartition : %s", topicName)
//		response = &protocol.Server2Client{
//			Key: protocol.Server2ClientKey_SendPartions,
//			Topic: topicName,
//			Partitions: topic.getPartitions(),
//		}
//	} else {
//		myLogger.Logger.Printf("Partition Not existed : %s", topicName)
//		response = &protocol.Server2Client{
//			Key: protocol.Server2ClientKey_TopicNotExisted,
//		}
//	}
//	return response
//}
//func (b *Broker)  getConsumerPartition(data *readData)   (response *protocol.Server2Client) {
//	if !b.isController{
//		myLogger.Logger.PrintWarning("try to getConsumerPartition int normal Broker")
//		return nil
//	}
//	request := data.client2serverData
//	groupName := request.GroupName
//	group, ok := b.getGroup(&groupName)
//	myLogger.Logger.Printf("registerComsummer : %s", groupName)
//	if !ok {
//		return nil
//		//group = newGroup(groupName)
//		//b.addGroup(group)
//	}
//
//	partitions := group.getClientPartition(data.clientID)
//	if partitions == nil{//不存在
//		return nil
//	}
//	response = &protocol.Server2Client{
//		Key: protocol.Server2ClientKey_ChangeConsumerPartition,
//		Partitions: partitions,
//	}
//	return response
//}

//func (b *Broker)  subscribeTopic(data *readData)   (response *protocol.Server2Client) {
//	if !b.isController{
//		myLogger.Logger.PrintWarning("try to subscribeTopic int normal Broker")
//		return nil
//	}
//	request := data.client2serverData
//	topicName := request.Topic
//	topic, ok := b.getTopic(&topicName)
//	if !ok {
//		myLogger.Logger.Printf("Topic Not existed : %s", topicName)
//		response = &protocol.Server2Client{
//			Key: protocol.Server2ClientKey_Error,
//		}
//		return response
//	}
//
//	groupName := request.GroupName
//	group, ok := b.getGroup(&groupName)
//	myLogger.Logger.Printf("registerComsummer : %s", groupName)
//	if !ok {
//		myLogger.Logger.Printf("newGroup : %s", groupName)
//		group = newGroup(groupName, 0, b)
//		b.addGroup(group)
//	}
//	clientConn, ok := b.getClient(data.clientID)
//	if !ok {
//		myLogger.Logger.Print("clientConn have close")
//	}
//
//	clientConn.belongGroup = groupName
//	succ:= group.addTopic(topic)
//	succ = group.addClient(clientConn) || succ
//	if succ{
//		group.Rebalance()
//		return nil
//	}else{
//		group.Rebalance()
//		return nil
//		//response = &protocol.Server2Client{
//		//	Key: protocol.Server2ClientKey_TopicExisted,
//		//}
//		//return response
//	}
//}
//
//
//func (b *Broker)  subscribePartition(data *readData)   (response *protocol.Server2Client) {
//	request := data.client2serverData
//	//topicName := request.Topic
//	partitionName := request.Partition
//	groupName := request.GroupName
//	b.partitionMapLock.RLock()
//	defer b.partitionMapLock.RUnlock()
//	partition, ok := b.getPartition(&partitionName)
//	if !ok {
//		myLogger.Logger.Printf("Partition Not existed : %s", partitionName)
//		response = &protocol.Server2Client{
//			Key: protocol.Server2ClientKey_Error,
//		}
//		return response
//	}else{
//		if partition.getGroupRebalanceId(groupName) > request.RebalanceId{//不是最新的，丢弃
//			myLogger.Logger.Printf("Reject subscribePartition oldId : %d newId: %d", partition.getGroupRebalanceId(groupName), request.RebalanceId)
//			return nil
//		}
//		clientConn, ok := b.getClient(data.clientID)
//		if !ok {
//			myLogger.Logger.Print("clientConn have close")
//		}
//		clientConn.consumePartions[partitionName] = true
//		partition.addComsummerClient(clientConn, groupName, request.RebalanceId)
//		response = &protocol.Server2Client{
//			Key: protocol.Server2ClientKey_Success,
//		}
//		clientConn.belongGroup = groupName
//		return response
//	}
//}
//
//func (b *Broker)  registerConsumer(data *readData)   (response *protocol.Server2Client) {
//	request := data.client2serverData
//	groupName := request.GroupName
//	group, ok := b.getGroup(&groupName)
//	myLogger.Logger.Printf("registerComsummer : %s", groupName)
//	if !ok {
//		group = newGroup(groupName, 0, b)
//		b.addGroup(group)
//	}
//	clientConn, ok := b.getClient(data.clientID)
//	if !ok {
//		myLogger.Logger.Print("clientConn have close")
//	}
//	succ := group.addClient(clientConn)
//	if succ{//已通过go balance response
//		return nil
//	}
//	response = &protocol.Server2Client{
//		Key: protocol.Server2ClientKey_ChangeConsumerPartition,
//		Partitions: group.getClientPartition(clientConn.id),
//	}
//	return response
//}
//
//func (b *Broker)  unRegisterConsumer(data *readData)   (response *protocol.Server2Client) {
//	request := data.client2serverData
//	groupName := request.GroupName
//	group, ok := b.getGroup(&groupName)
//	myLogger.Logger.Printf("registerComsummer : %s", groupName)
//	if !ok {
//		myLogger.Logger.Printf("group not exist : %s", groupName)
//		return nil
//	}
//	group.deleteClient(data.clientID)
//	return nil
//}
