package broker
import(
	"../protocol"
)
//type priorityQueue struct {
//	priorityQueue []*protocol.Message
//}


type priorityQueue []*protocol.InternalMessage
//var p priorityQueue
//var prio heap.Interface = &p

func NewPriorityQueue(capacity int) *priorityQueue{
	priorityQueue := make(priorityQueue, 0, capacity)
	return &priorityQueue
}

func (pq priorityQueue) Len() int{
	return len(pq)
}

func (pq priorityQueue) Less(i, j int) bool {
	return pq[i].Priority >= pq[j].Priority
}

func (pq priorityQueue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
	//pq[i].Pos = int32(i)
	//pq[j].Pos = int32(j)
}

func (pq *priorityQueue) Push(x interface{}) {
	n := len(*pq)
	mes := x.(*protocol.InternalMessage)
	mes.Pos = int32(n)
	*pq = append(*pq, mes)
}

func (pq *priorityQueue) Pop() interface{}{
	old := *pq
	n := len(old)
	mes := old[n-1]
	*pq = old[0 : n-1]
	return mes
}


