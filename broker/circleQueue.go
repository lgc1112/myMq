package broker

import (
	"../protocol"
	"errors"
)

type circleQueue struct {
	capacity int    //最后元素
	curSize int
	curSentSize int
	array   []*protocol.InternalMessage //数组
	head    int    //指向队列队首
	tail    int    //指向队尾
	sentIdx int //指向发送位置
}

func NewCircleQueue(capacity int) *circleQueue{
	cq := &circleQueue{
		capacity: capacity,
		array: make([]*protocol.InternalMessage, capacity),
	}
	return cq
}

//添加 入队列 Push(push)
func (c *circleQueue) Push(val *protocol.InternalMessage) (err error) {
	if c.IsFull() {
		return errors.New("queue full")
	}
	c.curSize++
	c.curSentSize++
	//this.tail在队列尾部，不包含最后的元素
	c.array[c.tail] = val
	c.tail = (c.tail + 1) % c.capacity
	return
}

//发送数据出队
func (c *circleQueue) PopSendData() (val *protocol.InternalMessage, err error) {
	if c.curSentSize == 0 {
		return nil, errors.New("queue empty")
	}
	c.curSentSize--
	val = c.array[c.sentIdx]
	c.sentIdx = (c.sentIdx + 1) % c.capacity
	return val, nil
}

//查看队首元素
func (c *circleQueue) Peek() (val *protocol.InternalMessage, err error) {
	if c.IsEmpty() {
		return nil, errors.New("queue empty")
	}
	//head是指向队首，且包含队首元素
	val = c.array[c.head]
	return val, nil
}

//取出环形队列有多少个元素
func (c *circleQueue) SentDataLen() int {
	return c.curSentSize
}

//将发送位移重置为队首元素
func (c *circleQueue) ResetSendData() {
	c.curSentSize = c.curSize
	c.sentIdx = c.head
}

//出队列
func (c *circleQueue) Pop() (val *protocol.InternalMessage, err error) {
	if c.IsEmpty() {
		return nil, errors.New("queue empty")
	}
	c.curSize--

	//head是指向队首，且包含队首元素
	val = c.array[c.head]
	c.head = (c.head + 1) % c.capacity
	return val, nil
}

//判断环形队列是否为满
func (c *circleQueue) IsFull() bool {
	return c.curSize == c.capacity
}

//判断是否为空
func (c *circleQueue) IsEmpty() bool {
	return c.curSize == 0
}

//取出环形队列有多少个元素
func (c *circleQueue) Len() int {
	return c.curSize
}
