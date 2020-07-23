package broker

import (
	"../protocol"
)

type inflightQueue []*protocol.InternalMessage
//var p priorityQueue
//var prio heap.Interface = &p

func NewInflightQueue(capacity int) *inflightQueue{
	inflightQueue := make(inflightQueue, 0, capacity)
	return &inflightQueue
}

func (iq inflightQueue) Len() int{
	return len(iq)
}

func (iq inflightQueue) Less(i, j int) bool {
	return iq[i].Priority > iq[j].Priority
}

func (iq inflightQueue) Swap(i, j int) {
	iq[i], iq[j] = iq[j], iq[i]
	iq[i].Pos = int32(i)
	iq[j].Pos = int32(j)
}

func (iq *inflightQueue) Push(x interface{}) {
	n := len(*iq)
	mes := x.(*protocol.InternalMessage)
	mes.Pos = int32(n)
	*iq = append(*iq, mes)
}

func (iq *inflightQueue) Pop() interface{}{
	old := *iq
	n := len(old)
	mes := old[n-1]
	*iq = old[0 : n-1]
	return mes
}


