package consumer


import (
	"../mylib/myLogger"
	"../protocol"
	"encoding/binary"
	"github.com/golang/protobuf/proto"
	"log"
)

type Consumer struct {
	addr   string
	groupName string
	conn *consumerConn
	partitions []*protocol.Partition
	sendIdx int
}
const logDir string = "./consumer/log/"
func NewConsumer(addr, groupName string) (*Consumer, error) {
	_, err := myLogger.New(logDir)
	p := &Consumer{
		addr: addr,
		groupName: groupName,
	}

	return p, err
}

func (c *Consumer) Connect2Broker() error {
	var err error
	c.conn, err = newConn(c.addr, c)

	if err != nil {
		//c.conn.Close()
		myLogger.Logger.Printf("(%s) error connecting to broker - %s %s", c.addr, err)
		return err
	}
	myLogger.Logger.Printf("connecting to broker - %s", c.addr)
	return nil
}

func (c *Consumer) SubscribeTopic(topicName string) error {
	requestData := &protocol.Client2Server{
		Key: protocol.Client2ServerKey_SubscribeTopic,
		Topic: topicName,
		GroupName: c.groupName,
	}
	data, err := proto.Marshal(requestData)
	if err != nil {
		log.Fatal("marshaling error: ", err)
	}
	var buf [4]byte
	bufs := buf[:]
	binary.BigEndian.PutUint32(bufs, uint32(len(data)))
	c.conn.Writer.Write(bufs)
	c.conn.Writer.Write(data)
	c.conn.Writer.Flush()
	myLogger.Logger.Printf("subscribeTopic %s : %s", topicName, requestData)

	response, err:= c.conn.readSeverData()

	if err == nil{
		myLogger.Logger.Printf("subscribeTopic response %s", response)
		c.partitions = response.Partitions
		c.subscribePartion()
	}else{
		myLogger.Logger.Printf("subscribeTopic err", err)
	}
	return err
}


func (c *Consumer) ReadLoop() {
	for{
		myLogger.Logger.Print("readLoop");
		c.conn.readSeverData()
	}
}
func (c *Consumer) subscribePartion(){
	myLogger.Logger.Print("subscribePartion len:", len(c.partitions))
	for _, partition := range c.partitions{
		requestData := &protocol.Client2Server{
			Key: protocol.Client2ServerKey_SubscribePartion,
			Topic: partition.TopicName,
			Partition: partition.Name,
			GroupName: c.groupName,
		}
		data, err := proto.Marshal(requestData)
		if err != nil {
			log.Fatal("marshaling error: ", err)
		}
		var buf [4]byte
		bufs := buf[:]
		binary.BigEndian.PutUint32(bufs, uint32(len(data)))
		c.conn.Writer.Write(bufs)
		c.conn.Writer.Write(data)
		c.conn.Writer.Flush()
		myLogger.Logger.Printf("subscribePartion %s", requestData)
	}


}

