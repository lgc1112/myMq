package producer

import (
	"../mylib/myLogger"
	"../protocol"
	"encoding/binary"
	"errors"
	"github.com/golang/protobuf/proto"
	"log"
)

type Producer struct {
	Addr   string
	conn *producerConn
	partitionMap map[string] []*protocol.Partition
	sendIdx int
	connectState int32
}
const logDir string = "./producer/log/"
func NewProducer(addr string) (*Producer, error) {
	_, err := myLogger.New(logDir)
	p := &Producer{
		Addr: addr,
		partitionMap: make(map[string] []*protocol.Partition),
	}

	return p, err
}

func (p *Producer) Connect2Broker() error {
	var err error
	p.conn, err = newConn(p.Addr, p)

	if err != nil {
		//p.conn.Close()
		myLogger.Logger.Printf(" connecting to broker error: %s %s", p.Addr, err)
		//atomic.StoreInt32(p.connectState)
		return err
	}
	myLogger.Logger.Printf("connecting to broker - %s", p.Addr)
	return nil
}

func (p *Producer) CreatTopic(topic string, num int32){
	requestData := &protocol.Client2Server{
		Key: protocol.Client2ServerKey_CreatTopic,
		Topic: topic,
		PartitionNum: num,
	}
	data, err := proto.Marshal(requestData)
	if err != nil {
		log.Fatal("marshaling error: ", err)
	}
	var buf [4]byte
	bufs := buf[:]
	binary.BigEndian.PutUint32(bufs, uint32(len(data)))
	p.conn.Writer.Write(bufs)
	p.conn.Writer.Write(data)
	p.conn.Writer.Flush()
	myLogger.Logger.Printf("CreatTopic %s : %d", topic, num)

	response, err:= p.conn.readResponse()
	if err == nil{
		p.partitionMap[topic] = response.Partitions
	}
}

func (p *Producer) getPartition(topic string) (*protocol.Partition){//循环读取
	partitions, ok := p.partitionMap[topic]
	if !ok {
		myLogger.Logger.Printf("topic not exist", topic)
		err := p.getTopicPartition(topic)
		if err != nil {
			myLogger.Logger.Print("getPartition error: ", err)
			return nil
		}
	}
	len := len(p.partitionMap[topic])
	if len == 0{
		myLogger.Logger.Print("getPartition err")
		return nil
	}
	p.sendIdx++;
	if p.sendIdx >= len{
		p.sendIdx %= len
	}
	return partitions[p.sendIdx]
}
func (p *Producer) Pubilsh(topic string, data []byte, prioroty int32) error{
	msg := &protocol.Message{
		Priority: prioroty,
		Msg: data,
	}
	partition := p.getPartition(topic)
	if partition == nil{
		return errors.New("cannot get partition")
	}
	requestData := &protocol.Client2Server{
		Key: protocol.Client2ServerKey_Publish,
		Topic: topic,
		Partition:partition.Name,
		Msg: msg,
	}
	data, err := proto.Marshal(requestData)
	if err != nil {
		log.Fatal("marshaling error: ", err)
	}
	var buf [4]byte
	bufs := buf[:]
	binary.BigEndian.PutUint32(bufs, uint32(len(data)))
	_, err = p.conn.Writer.Write(bufs)
	if err != nil {
		myLogger.Logger.PrintError("writer error: ", err)
		return err
	}
	_, err = p.conn.Writer.Write(data)
	if err != nil {
		myLogger.Logger.PrintError("writer error: ", err)
		return err
	}
	err = p.conn.Writer.Flush()
	if err != nil {
		myLogger.Logger.PrintError("writer error: ", err)
		return err
	}
	//p.conn.conn.Close()
	myLogger.Logger.Printf("Pubilsh %s", requestData)
	response, err:= p.conn.readResponse()
	if err == nil{
		if response.Key == protocol.Server2ClientKey_PublishSuccess{
			myLogger.Logger.Print("PublishSuccess")
		}
	}else{
		myLogger.Logger.Print("PublishError")
	}

	return err
}

func (p *Producer) getTopicPartition(topic string) error{
	requestData := &protocol.Client2Server{
		Key: protocol.Client2ServerKey_GetPublisherPartition,
		Topic: topic,
	}
	data, err := proto.Marshal(requestData)
	if err != nil {
		myLogger.Logger.Print("marshaling error: ", err)
		return err
	}
	var buf [4]byte
	bufs := buf[:]
	binary.BigEndian.PutUint32(bufs, uint32(len(data)))
	p.conn.Writer.Write(bufs)
	p.conn.Writer.Write(data)
	p.conn.Writer.Flush()
	myLogger.Logger.Printf("GetTopicPartion %s : %s", topic, requestData)

	response, err:= p.conn.readResponse()
	if err == nil{
		p.partitionMap[topic] = response.Partitions
	}
	return err
}

