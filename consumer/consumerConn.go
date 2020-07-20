package consumer

import (
	"../mylib/myLogger"
	"../protocol"
	"bufio"
	"encoding/binary"
	"github.com/golang/protobuf/proto"
	"io"
	"net"
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
func (p *consumerConn)readSeverData() (*protocol.Server2Client, error){
	myLogger.Logger.Print("readResponse...")
	tmp := make([]byte, 4)
	_, err := io.ReadFull(p.Reader, tmp) //读取长度
	if err != nil {
		if err == io.EOF {
			myLogger.Logger.Print("EOF")
		} else {
			myLogger.Logger.Print(err)
		}
		return nil, err
	}
	len := int32(binary.BigEndian.Uint32(tmp))
	myLogger.Logger.Printf("readLen %d ", len)
	requestData := make([]byte, len)
	_, err = io.ReadFull(p.Reader, requestData) //读取内容
	if err != nil {
		if err == io.EOF {
			myLogger.Logger.Print("EOF")
		} else {
			myLogger.Logger.Print(err)
		}
		return nil, err
	}
	response := &protocol.Server2Client{}
	err = proto.Unmarshal(requestData, response)
	if err != nil {
		myLogger.Logger.Print("Unmarshal error %s", err)
		return nil, err
	}
	myLogger.Logger.Printf("receive response: %s", response)
	return response, nil

}