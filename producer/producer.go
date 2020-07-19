package producer

import (
	"../mylib/myLogger"
	"../protocol"
	"encoding/binary"
	"github.com/golang/protobuf/proto"
	"log"
)

type Producer struct {
	addr   string
	conn *producerConn
	loger *myLogger.MyLogger
}
const logDir string = "./producer/log/"
func NewProducer(addr string) (*Producer, error) {
	loger, err := myLogger.New(logDir)
	p := &Producer{
		addr: addr,
		loger: loger,
	}

	return p, err
}

func (p *Producer) Connect2Broker() error {
	var err error
	p.conn, err = newConn(p.addr)

	if err != nil {
		//p.conn.Close()
		p.loger.Printf("(%s) error connecting to broker - %s %s", p.addr, err)
		return err
	}
	p.loger.Printf("connecting to broker - %s", p.addr)
	return nil
}

func (p *Producer) CreatTopic(topic string, num int32){
	requestData := &protocol.Request{
		Key: protocol.RequestKey_CreatTopic,
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
	p.loger.Printf("CreatTopic %s : %d", topic, num)
}

func (p *Producer) GetProducerPartitionForTopic(topic string){

}

func (p *Producer) publish(topic string, msg []byte, priority int32){

}
