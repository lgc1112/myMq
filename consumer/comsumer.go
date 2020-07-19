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
	conn *consumerConn
	logger *myLogger.MyLogger
	partitions []*protocol.Partition
	sendIdx int
}
const logDir string = "./consumer/log/"
func NewConsumer(addr string) (*Consumer, error) {
	loger, err := myLogger.New(logDir)
	p := &Consumer{
		addr: addr,
		logger: loger,
	}

	return p, err
}

func (p *Consumer) Connect2Broker() error {
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

func (p *Consumer) CreatTopic(topic string, num int32){
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
	p.logger.Printf("CreatTopic %s : %d", topic, num)

	response, err:= p.conn.readResponse()
	if err == nil{
		p.partitions = response.Partitions
	}
}
func (p *Consumer) ReadLoop(){
	for{
		p.conn.readResponse()

	}
}
func (p *Consumer) GetTopicPartion(topic string){
	requestData := &protocol.Request{
		Key: protocol.RequestKey_GetPublisherPartition,
		Topic: topic,
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
	p.logger.Printf("GetTopicPartion %s : %s", topic, requestData)

	response, err:= p.conn.readResponse()
	if err == nil{
		p.partitions = response.Partitions
	}
}

func (p *Consumer) getPartition() (*protocol.Partition){
	len := len(p.partitions)
	if len == 0{
		p.logger.Print("getPartition err")
		return nil
	}
	if p.sendIdx >= len{
		p.sendIdx %= len
	}
	return p.partitions[p.sendIdx]
}
func (p *Consumer) Pubilsh(topic string, data []byte, prioroty int32){
	msg := &protocol.Message{
		Priority: prioroty,
		Msg: data,
	}

	requestData := &protocol.Request{
		Key: protocol.RequestKey_Publish,
		Topic: topic,
		Partition:p.getPartition().Name,
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
}
func (p *Consumer) Subscribe(topic string){
	requestData := &protocol.Request{
		Key: protocol.RequestKey_Subscribe,
		Topic: topic,
		Partition:p.getPartition().Name,
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
}


func (p *Consumer) GetProducerPartitionForTopic(topic string){

}


