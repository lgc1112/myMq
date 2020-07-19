package broker

import (
	"strconv"
	"sync"
	"../protocol"
)

type topic struct {
	sync.RWMutex
	broker *Broker
	name string
	partitionMapLock sync.RWMutex
	partitionMap map[string]*partition
	maxPartitionNum int
}

func newTopic(name string, partitionNum int, broker *Broker) *topic {
	t := &topic{
		broker : broker,
		name : name,
		partitionMap : make(map[string]*partition),
	}

	addr := broker.addr
	for i := 0; i < partitionNum; i++{
		partitionName := name + "-" + strconv.Itoa(i)
		partition := newPartition(partitionName, addr)
		t.partitionMapLock.Lock()
		t.partitionMap[partitionName] = partition
		t.partitionMapLock.Unlock()
	}
	t.maxPartitionNum = partitionNum

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
	t.partitionMapLock.RUnlock();
	return partitions
}