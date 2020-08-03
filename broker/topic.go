package broker

import (
	"../mylib/myLogger"
	"../protocol"
	"strconv"
	"sync"
	"sync/atomic"
)

type topic struct {
	broker *Broker
	name string
	partitionMapLock sync.RWMutex
	partitionMap map[string]*partition
	maxPartitionNum int64
}

func newTopic(name string, broker *Broker) *topic {
	t := &topic{
		broker : broker,
		name : name,
		partitionMap : make(map[string]*partition),
	}
	return t
}

func (t *topic) deleteAllPartitions(){
	//addr := broker.maddr.ClientListenAddr
	t.partitionMapLock.Lock()
	defer t.partitionMapLock.Unlock()
	for _, partition := range t.partitionMap{
		if !partition.isNativePartition{ //不是本地分区，通知broker关闭
			brokerConn, ok := t.broker.GetBrokerConn(&partition.addr)
			if !ok {
				myLogger.Logger.PrintWarning("delete partition is not alive")
			}else{
				controller2BrokerData := &protocol.Controller2Broker{ //创建分区的消息
					Key: protocol.Controller2BrokerKey_DeletePartition,
					Partitions: &protocol.Partition{
						Name: partition.name,
						Addr: partition.addr,
					},
				}
				err := brokerConn.Put(controller2BrokerData) //发送
				if err != nil{ //该broker无法分配分区，换下一个
					myLogger.Logger.PrintWarning("delete partition cannot contact")
				}
			}
		}
		t.broker.deletePartition(&partition.name) //先删除，再退出
		partition.exit()
	}

	return
}

func (t *topic) CreatePartitions(partitionNum int, addrs []string){
	//addr := broker.maddr.ClientListenAddr
	k := 0
	addrNum := len(addrs)
	for i := 0; i < partitionNum; i++{
		partitionName := t.name + "-" + strconv.Itoa(i)
		addr := addrs[k % addrNum]
		k++
		isNativePartition := addr == t.broker.maddr.ClientListenAddr

		if !isNativePartition{
			brokerConn, _ := t.broker.GetBrokerConn(&addr)
			controller2BrokerData := &protocol.Controller2Broker{ //创建分区的消息
				Key: protocol.Controller2BrokerKey_CreadtPartition,
				Partitions: &protocol.Partition{
					Name: partitionName,
					Addr: addr,
				},
			}
			err := brokerConn.Put(controller2BrokerData) //发送
			if err != nil{ //该broker无法分配分区，换下一个
				i--
				continue
			}
		}
		partition := newPartition(partitionName, addr, isNativePartition, t.broker) //将分区轮询分配给不同的addr
		t.partitionMapLock.Lock()
		t.partitionMap[partitionName] = partition
		t.partitionMapLock.Unlock()
		t.broker.addPartition(&partitionName, partition)
	}
	atomic.StoreInt64(&t.maxPartitionNum, int64(partitionNum))
	return
}

func (t *topic) getPartition(partitionName *string) (*partition, bool) {
	t.partitionMapLock.RLock()
	partition, ok := t.partitionMap[*partitionName]
	t.partitionMapLock.RUnlock()
	return partition, ok
}

func (t *topic) AddPartition(partition *partition){
	t.partitionMapLock.Lock()
	t.partitionMap[partition.name] = partition
	t.partitionMapLock.Unlock()
	return
}

func (t *topic)getPartitions() []*protocol.Partition {
	var partitions []*protocol.Partition
	t.partitionMapLock.RLock()
	for _, par := range t.partitionMap{
		tmp := &protocol.Partition{
			Name: par.name,
			Addr: par.addr,
		}
		partitions = append(partitions, tmp)
	}
	t.partitionMapLock.RUnlock()
	return partitions
}