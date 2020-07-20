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
	addr   string
	conn *producerConn
	logger *myLogger.MyLogger
	partitionMap map[string] []*protocol.Partition
	sendIdx int
}
const logDir string = "./producer/log/"
func NewProducer(addr string) (*Producer, error) {
	loger, err := myLogger.New(logDir)
	p := &Producer{
		addr: addr,
		logger: loger,
		partitionMap: make(map[string] []*protocol.Partition),
	}

	return p, err
}

func (p *Producer) Connect2Broker() error {
	var err error
	p.conn, err = newConn(p.addr, p)

	if err != nil {
		//p.conn.Close()
		p.logger.Printf("(%s) error connecting to broker - %s %s", p.addr, err)
		return err
	}
	p.logger.Printf("connecting to broker - %s", p.addr)
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
	p.logger.Printf("CreatTopic %s : %d", topic, num)

	response, err:= p.conn.readResponse()
	if err == nil{
		p.partitionMap[topic] = response.Partitions
	}
}

func (p *Producer) getPartition(topic string) (*protocol.Partition){//循环读取
	partitions, ok := p.partitionMap[topic]
	if !ok {
		p.logger.Printf("topic not exist", topic)
		err := p.getTopicPartition(topic)
		if err != nil {
			p.logger.Print("getPartition error: ", err)
			return nil
		}
	}
	len := len(p.partitionMap[topic])
	if len == 0{
		p.logger.Print("getPartition err")
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
	p.conn.Writer.Write(bufs)
	p.conn.Writer.Write(data)
	p.conn.Writer.Flush()
	p.logger.Printf("Pubilsh %s", requestData)
	return nil
}

func (p *Producer) getTopicPartition(topic string) error{
	requestData := &protocol.Client2Server{
		Key: protocol.Client2ServerKey_GetPublisherPartition,
		Topic: topic,
	}
	data, err := proto.Marshal(requestData)
	if err != nil {
		p.logger.Print("marshaling error: ", err)
		return err
	}
	var buf [4]byte
	bufs := buf[:]
	binary.BigEndian.PutUint32(bufs, uint32(len(data)))
	p.conn.Writer.Write(bufs)
	p.conn.Writer.Write(data)
	p.conn.Writer.Flush()
	p.logger.Printf("GetTopicPartion %s : %s", topic, requestData)

	response, err:= p.conn.readResponse()
	if err == nil{
		p.partitionMap[topic] = response.Partitions
	}
	return err
}

