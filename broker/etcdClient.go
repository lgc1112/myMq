package broker


import (
	"../mylib/myLogger"
	"context"
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
		myLogger.Logger.Print(err)
		return err
	}
	return nil
}

func (e *etcdClient)watcher()  {
	//kv := clientv3.NewKV(e.client)
	// 先GET到当前的值，并监听后续变化
	var getResp *clientv3.GetResponse
	var err error
	if getResp, err = e.client.Get(context.TODO(), masterAddrKey); err != nil {
		myLogger.Logger.Print(err)
		return
	}
	// 现在key是存在的
	if len(getResp.Kvs) != 0 {
		myLogger.Logger.Print("当前值:", string(getResp.Kvs[0].Value))
	}
	// 获得当前revision
	watchStartRevision := getResp.Header.Revision + 1
	// 创建一个watcher
	myLogger.Logger.Print("从该版本向后监听:", watchStartRevision)

	watcher := clientv3.NewWatcher(e.client)

	ctx, _ := context.WithCancel(context.TODO())

	//time.AfterFunc(5 * time.Second, func() {
	//	cancelFunc()
	//})


	// 处理kv变化事件
	watchRespChan := watcher.Watch(ctx, masterAddrKey)
	myLogger.Logger.Print("startWatch...")
	for{
		select {
		case watchResp := <-watchRespChan:
			for _, event := range watchResp.Events {
				switch event.Type {
				case mvccpb.PUT:
					myLogger.Logger.Print("修改为:", string(event.Kv.Value), "Revision:", event.Kv.CreateRevision, event.Kv.ModRevision, " leaderFlag:", e.IsLeader)
				case mvccpb.DELETE:
					myLogger.Logger.Print("删除了", "Revision:", event.Kv.ModRevision)
				default:
					myLogger.Logger.Print("efwf")
				}
			}
		}
	}
	//for watchResp := range watchRespChan {
	//	for _, event := range watchResp.Events {
	//		switch event.Type {
	//		case mvccpb.PUT:
	//			myLogger.Logger.Println("修改为:", string(event.Kv.Value), "Revision:", event.Kv.CreateRevision, event.Kv.ModRevision, "leaderFlag:", e.leaderFlag)
	//		case mvccpb.DELETE:
	//			myLogger.Logger.Println("删除了", "Revision: ", event.Kv.ModRevision)
	//		default:
	//			myLogger.Logger.Println("undefined")
	//		}
	//	}
	//}
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
		var putResp *clientv3.PutResponse
		if putResp, err = e.client.Put(context.TODO(), masterAddrKey, e.broker.addr, clientv3.WithPrevKV()); err != nil {
			myLogger.Logger.Print(err)
			return
		}
		myLogger.Logger.Print(putResp.Header.Revision)
		e.broker.BecameController()
		//if putResp.PrevKv != nil {
		//	myLogger.Logger.Printf("prev Value: %s \n CreateRevision : %d \n ModRevision: %d \n Version: %d \n",
		//		string(putResp.PrevKv.Value), putResp.PrevKv.CreateRevision, putResp.PrevKv.ModRevision, putResp.PrevKv.Version)
		//}
		select {
		case <-s.Done():
			e.IsLeader = false
			e.broker.BecameNormalBroker()
			myLogger.Logger.Print("elect: expired")
		}
	}
}
