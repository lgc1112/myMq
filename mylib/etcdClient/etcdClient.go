package etcdClient

import (
	"../../protocol"
	"../myLogger"
	"context"
	"github.com/golang/protobuf/proto"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/clientv3/concurrency"
	"go.etcd.io/etcd/mvcc/mvccpb"
	"time"
)

const prefix = "/election"
const controllerAddrKey = "/controllerAddr"
const metaDataKey = "/metaData"
const campaignTTL = 10 //竞选节点维护时间
//
//var endpoints = []string{defaultEtcdAddr}
type EtcdListener interface {
	ChangeControllerAddr(*protocol.ListenAddr)
	BecameNormalBroker()
	BecameController()
}

type EtcdClient struct {
	broker   EtcdListener
	client   *clientv3.Client
	IsLeader bool
}

//新建etcd连接
func NewEtcdClient(broker EtcdListener, etcdAddr *string) (*EtcdClient, error) {
	client, err := clientv3.New(clientv3.Config{Endpoints: []string{*etcdAddr}, DialTimeout: 5 * time.Second})
	if err != nil {
		myLogger.Logger.Print(err)
		return nil, err
	}
	//kv := clientv3.NewKV(client)
	e := &EtcdClient{
		broker: broker,
		client: client,
	}
	go e.campaign(prefix, "1") //竞选controller协程
	go e.watcher()             //监听controller地址变化协程
	return e, nil
}

//保存集群元数据
func (e *EtcdClient) PutMetaData(val string) error {
	//var putResp *clientv3.PutResponse
	if _, err := e.client.Put(context.TODO(), metaDataKey, val); err != nil {
		myLogger.Logger.PrintError(err)
		return err
	}
	return nil
}

//修改topic的分区
func (e *EtcdClient) PutPatitions(key string, val string) error {
	//var putResp *clientv3.PutResponse
	if _, err := e.client.Put(context.TODO(), topicAddrKey+"/"+key, val); err != nil {
		myLogger.Logger.PrintError(err)
		return err
	}
	return nil
}

//修改controller地址
func (e *EtcdClient) PutControllerAddr(addr *protocol.ListenAddr) error {
	//var putResp *clientv3.PutResponse
	data, err := proto.Marshal(addr)
	if _, err = e.client.Put(context.TODO(), controllerAddrKey, string(data)); err != nil {
		myLogger.Logger.PrintError(err)
		return err
	}
	//myLogger.Logger.Print(putResp.Header.Revision)
	return nil
}

//获取controller地址
func (e *EtcdClient) GetControllerAddr() (*protocol.ListenAddr, error) {
	var getResp *clientv3.GetResponse
	var err error
	if getResp, err = e.client.Get(context.TODO(), controllerAddrKey); err != nil {
		myLogger.Logger.PrintError(err)
		return nil, err
	}

	listenAddr := &protocol.ListenAddr{}
	if len(getResp.Kvs) != 0 {
		err = proto.Unmarshal(getResp.Kvs[0].Value, listenAddr)
		if err != nil {
			myLogger.Logger.PrintError(err)
			return nil, err
		}
	}

	myLogger.Logger.Print("GetmasterAddr:", listenAddr)
	return listenAddr, nil
}

//清除元数据
func (e *EtcdClient) ClearMetaData() {
	if _, err := e.client.Delete(context.TODO(), metaDataKey); err != nil {
		myLogger.Logger.PrintError(err)
		return
	}
}

//获取元数据
func (e *EtcdClient) GetMetaData() ([]byte, error) {
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
	} else {
		return nil, nil
	}
}

//监听controller变化协程
func (e *EtcdClient) watcher() {

	watcher := clientv3.NewWatcher(e.client)

	ctx, _ := context.WithCancel(context.TODO())

	//time.AfterFunc(5 * time.Second, func() {
	//	cancelFunc()
	//})

	watchRespChan := watcher.Watch(ctx, controllerAddrKey)
	//watcher.Watch(ctx, controllerAddrKey)
	myLogger.Logger.Print("startWatch...")
	for {
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
					e.broker.ChangeControllerAddr(listenAddr)
				case mvccpb.DELETE:
					myLogger.Logger.Print("删除了", "Revision:", event.Kv.ModRevision)
				default:
					myLogger.Logger.Print(event.Type)
				}
			}
		}
	}
	myLogger.Logger.Print("bye")
}

//竞选协程
func (e *EtcdClient) campaign(election string, prop string) {
	for {
		s, err := concurrency.NewSession(e.client, concurrency.WithTTL(campaignTTL))
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

		e.broker.BecameController() //竞选成为controller

		select {
		case <-s.Done(): //是否变为普通broker
			e.IsLeader = false
			e.broker.BecameNormalBroker()
			myLogger.Logger.Print("elect: expired")
		}
	}
}
