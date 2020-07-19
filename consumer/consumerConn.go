package consumer

import (
	"bufio"
	"encoding/binary"
	"io"
	"net"
	"../protocol"
	"github.com/golang/protobuf/proto"
)

type consumerConn struct {
	addr string
	producer *Consumer
	Reader *bufio.Reader
	Writer *bufio.Writer
}

func newConn(addr string, producer *Consumer)  (*consumerConn, error){
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}
	c := &consumerConn{
		addr: addr,
		Reader: bufio.NewReader(conn),
		Writer: bufio.NewWriter(conn),
		producer: producer,
	}
	return c, nil
}
func (p *consumerConn)readResponse() (*protocol.Response, error){
	p.producer.logger.Print("readResponse");
	tmp := make([]byte, 4)
	_, err := io.ReadFull(p.Reader, tmp) //读取长度
	if err != nil {
		if err == io.EOF {
			p.producer.logger.Print("EOF")
		} else {
			p.producer.logger.Print(err)
		}
		return nil, err
	}
	len := int32(binary.BigEndian.Uint32(tmp))
	p.producer.logger.Printf("readLen %d ", len);
	requestData := make([]byte, len)
	_, err = io.ReadFull(p.Reader, requestData) //读取内容
	if err != nil {
		if err == io.EOF {
			p.producer.logger.Print("EOF")
		} else {
			p.producer.logger.Print(err)
		}
		return nil, err
	}
	response := &protocol.Response{}
	err = proto.Unmarshal(requestData, response)
	if err != nil {
		p.producer.logger.Print("Unmarshal error %s", err);
		return nil, err
	}
	p.producer.logger.Printf("receive request Key:%s : %s", response.ResponseType, response);
	return response, nil

}