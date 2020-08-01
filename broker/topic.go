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

func newTopic(name string, partitionNum int, broker *Broker) *topic {
	t := &topic{
		broker : broker,
		name : name,
		partitionMap : make(map[string]*partition),
	}

	addr := broker.addr

	broker.partitionMapLock.Lock()
	for i := 0; i < partitionNum; i++{
		partitionName := name + "-" + strconv.Itoa(i)
		partition := newPartition(partitionName, addr)
		t.partitionMapLock.Lock()
		t.partitionMap[partitionName] = partition
		t.partitionMapLock.Unlock()
		broker.addPartition(&partitionName, partition)
	}
	broker.partitionMapLock.Unlock()
	atomic.StoreInt64(&t.maxPartitionNum, int64(partitionNum))

	return t
}
func (t *topic) getPartition(partitionName *string) (*partition, bool) {
	t.partitionMapLock.RLock()
	partition, ok := t.partitionMap[*partitionName]
	t.partitionMapLock.RUnlock()
	return partition, ok
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