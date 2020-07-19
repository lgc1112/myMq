package broker

import "strconv"

type topic struct {
	broker *Broker
	name string
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
		t.partitionMap[partitionName] = partition
	}
	t.maxPartitionNum = partitionNum

	return t
}