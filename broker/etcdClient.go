package broker


import (
	"../mylib/myLogger"
	"../protocol"
	"context"
	"github.com/golang/protobuf/proto"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/clientv3/concurrency"
	"go.etcd.io/etcd/mvcc/mvccpb"
	"time"
)

const prefix = "/election"
const masterAddrKey = "/masterAddr"
const metaDataKey = "/metaData"
var endpoints = []string{"9.135.8.253:2379"}


type etcdClient struct{
	broker *Broker
	client *clientv3.Client
	IsLeader bool
}

func NewEtcdClient(broker *Broker) (*etcdClient, error){
	client, err := clientv3.New(clientv3.Config{Endpoints: endpoints, DialTimeout: 5 * time.Second})
	if err != nil {
		myLogger.Logger.Print(err)
		return nil, err
	}
	//kv := clientv3.NewKV(client)
	e := &etcdClient{
		broker: broker,
		client: client,
	}
	go e.campaign(prefix, "1")
	go e.watcher()
	return e, nil
}

func (e *etcdClient)PutMetaData(val string)  error{
	//var putResp *clientv3.PutResponse
	if _, err := e.client.Put(context.TODO(), metaDataKey, val); err != nil {
		myLogger.Logger.PrintError(err)
		return err
	}
	return nil
}

func (e *etcdClient)PutMasterAddr(addr *protocol.ListenAddr)  error{
	//var putResp *clientv3.PutResponse
	data, err := proto.Marshal(addr)
	if _, err = e.client.Put(context.TODO(), masterAddrKey, string(data)); err != nil {
		myLogger.Logger.PrintError(err)
		return err
	}
	//myLogger.Logger.Print(putResp.Header.Revision)
	return nil
}

func (e *etcdClient)GetmasterAddr()  (string, error){
	var getResp *clientv3.GetResponse
	var err error
	if getResp, err = e.client.Get(context.TODO(), masterAddrKey); err != nil {
		myLogger.Logger.PrintError(err)
		return "", err
	}

	listenAddr := &protocol.ListenAddr{}
	if len(getResp.Kvs) != 0 {
		err = proto.Unmarshal(getResp.Kvs[0].Value, listenAddr)
		if err != nil {
			myLogger.Logger.PrintError(err)
			return "", err
		}
	}
	// 获得当前revision
	//watchStartRevision := getResp.Header.Revision + 1

	myLogger.Logger.Print("GetmasterAddr:", listenAddr.BrokerListenAddr)
	//myLogger.Logger.Print("从该版本向后监听:", watchStartRevision)
	return listenAddr.BrokerListenAddr, nil
}

func (e *etcdClient)clearMetaData()  {
	if _, err := e.client.Delete(context.TODO(), metaDataKey, clientv3.WithPrevKV()); err != nil {
		myLogger.Logger.PrintError(err)
		return
	}
}
func (e *etcdClient)GetMetaData() ([]byte, error){
	//e.clearMetaData()
	var getResp *clientv3.GetResponse
	var err error
	if getResp, err = e.client.Get(context.TODO(), metaDataKey); err != nil {
		myLogger.Logger.PrintError(err)
		return nil, err
	}
	// 现在key是存在的
	if len(getResp.Kvs) != 0 {
		//myLogger.Logger.Print("当前值:", string(getResp.Kvs[0].Value))
		return getResp.Kvs[0].Value, nil
	}else{
		return nil, nil
	}
}

func (e *etcdClient)watcher()  {
	//e.GetmasterAddr()

	watcher := clientv3.NewWatcher(e.client)

	ctx, _ := context.WithCancel(context.TODO())

	//time.AfterFunc(5 * time.Second, func() {
	//	cancelFunc()
	//})

	watchRespChan := watcher.Watch(ctx, masterAddrKey)
	myLogger.Logger.Print("startWatch...")
	for{
		select {
		case watchResp := <-watchRespChan:
			for _, event := range watchResp.Events {
				switch event.Type {
				case mvccpb.PUT:
					myLogger.Logger.Print("修改为:", string(event.Kv.Value), "  Revision:", event.Kv.CreateRevision, event.Kv.ModRevision, " leaderFlag:", e.IsLeader)
					listenAddr := &protocol.ListenAddr{}
					err := proto.Unmarshal(event.Kv.Value, listenAddr)
					if err != nil {
						myLogger.Logger.PrintError(err)
						continue
					}
					myLogger.Logger.Print("当前值:", listenAddr)
					e.broker.ChangeMasterAddr(listenAddr.BrokerListenAddr)
				case mvccpb.DELETE:
					myLogger.Logger.Print("删除了", "Revision:", event.Kv.ModRevision)
				default:
					myLogger.Logger.Print("efwf")
				}
			}
		}
	}
	myLogger.Logger.Print("bye")
}

func (e *etcdClient)campaign( election string, prop string) {
	for {
		s, err := concurrency.NewSession(e.client, concurrency.WithTTL(15))
		if err != nil {
			myLogger.Logger.Print(err)
			continue
		}
		ele := concurrency.NewElection(s, election)
		ctx := context.TODO()

		if err = ele.Campaign(ctx, prop); err != nil {
			myLogger.Logger.Print(err)
			continue
		}

		myLogger.Logger.Print("elect: success")
		e.IsLeader = true //选举成功

		e.broker.BecameController() //竞选成为master
		//err := e.PutmasterAddr(e.broker.addr)


		select {
		case <-s.Done(): //是否变为普通broker
			e.IsLeader = false
			e.broker.BecameNormalBroker()
			myLogger.Logger.Print("elect: expired")
		}
	}
}
