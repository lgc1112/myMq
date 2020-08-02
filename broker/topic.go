package broker

import (
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

func (t *topic) CreatePartitions(partitionNum int, addrs []string){
	//addr := broker.maddr.ClientListenAddr
	k := 0
	addrNum := len(addrs)
	for i := int(atomic.LoadInt64(&t.maxPartitionNum)); i < partitionNum; i++{
		partitionName := t.name + "-" + strconv.Itoa(i)
		partition := newPartition(partitionName, addrs[k % addrNum], addrs[k % addrNum] == t.broker.maddr.ClientListenAddr) //将分区轮询分配给不同的addr
		t.partitionMapLock.Lock()
		t.partitionMap[partitionName] = partition
		t.partitionMapLock.Unlock()
		t.broker.addPartition(&partitionName, partition)
		k++
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