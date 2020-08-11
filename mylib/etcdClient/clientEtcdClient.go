package etcdClient

import (
	"../../protocol"
	"../myLogger"
	"context"
	"github.com/golang/protobuf/proto"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/mvcc/mvccpb"
	"strings"
	"time"
)

const topicAddrKey string = "/topic"

//监听回调接口
type ClientEtcdListener interface {
	ControllerAddrChange(*protocol.ListenAddr)
	TopicChange(topic string, partition *protocol.Partitions)
}

type ClientEtcdClient struct {
	handle ClientEtcdListener
	client *clientv3.Client
}

//客户端的etcd连接
func NewClientEtcdClient(clientListener ClientEtcdListener, etcdAddr *string) (*ClientEtcdClient, error) {
	client, err := clientv3.New(clientv3.Config{Endpoints: []string{*etcdAddr}, DialTimeout: 5 * time.Second})
	if err != nil {
		myLogger.Logger.Print(err)
		return nil, err
	}
	e := &ClientEtcdClient{
		handle: clientListener,
		client: client,
	}
	go e.watcher() //watch事件
	return e, nil
}

//获取当前controller的地址
func (c *ClientEtcdClient) GetControllerAddr() (*protocol.ListenAddr, error) {
	var getResp *clientv3.GetResponse
	var err error
	if getResp, err = c.client.Get(context.TODO(), controllerAddrKey); err != nil {
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

//监听controller或topic 修改的协程
func (c *ClientEtcdClient) watcher() {

	watcher := clientv3.NewWatcher(c.client)

	ctx, _ := context.WithCancel(context.TODO())

	watchRespChan := watcher.Watch(ctx, controllerAddrKey)
	//watcher.Watch(ctx, controllerAddrKey)
	topicWatchCh := watcher.Watch(context.Background(), topicAddrKey, clientv3.WithPrefix())
	myLogger.Logger.Print("startWatch...")
	for {
		select {
		case watchResp := <-topicWatchCh: //topic发生改变
			for _, event := range watchResp.Events {
				switch event.Type {
				case mvccpb.PUT:
					myLogger.Logger.Print("key:", string(event.Kv.Key), " Value:", string(event.Kv.Value), "  Revision:", event.Kv.CreateRevision, event.Kv.ModRevision)
					partitions := &protocol.Partitions{}
					err := proto.Unmarshal(event.Kv.Value, partitions)
					if err != nil {
						myLogger.Logger.PrintError(err)
						continue
					}
					l3 := strings.Count(topicAddrKey, "")
					c.handle.TopicChange(string(event.Kv.Key[l3:]), partitions)
				case mvccpb.DELETE:
					myLogger.Logger.Print("删除了", "Revision:", event.Kv.ModRevision)
				default:
					myLogger.Logger.Print("efwf")
				}
			}
		case watchResp := <-watchRespChan: //Controller发生改变
			for _, event := range watchResp.Events {
				switch event.Type {
				case mvccpb.PUT:
					myLogger.Logger.Print("修改为:", string(event.Kv.Value), "  Revision:", event.Kv.CreateRevision, event.Kv.ModRevision)
					listenAddr := &protocol.ListenAddr{}
					err := proto.Unmarshal(event.Kv.Value, listenAddr)
					if err != nil {
						myLogger.Logger.PrintError(err)
						continue
					}
					myLogger.Logger.Print("当前值:", listenAddr)
					c.handle.ControllerAddrChange(listenAddr)
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
