package protocalFuc

import (
	"../../mylib/myLogger"
	"../../protocol"
	"encoding/binary"
	"github.com/golang/protobuf/proto"
	"io"
)

//根据协议打包数据为[]byte
func PackClientServerProtoBuf(cmd protocol.ClientServerCmd, body []byte) ([]byte, error) {
	h := &protocol.ClientServerHeader{ //消息头创建
		Cmd:    cmd,
		MsgLen: int32(len(body)),
	}
	//client2ServerData := &protocol.Message{}
	//h = client2ServerData.(*protocol.ClientServerHeader)
	hBytes, err := proto.Marshal(h) //消息头转字节数组
	if err != nil {
		myLogger.Logger.PrintError("marshaling error: ", err)
		return nil, err
	}
	res := make([]byte, 4)
	binary.BigEndian.PutUint32(res, uint32(len(hBytes))) //放入消息头长度

	res = append(res, hBytes...) //放入消息头
	res = append(res, body...)   //放入消息体
	return res, nil
}

//读取并解析协议
func ReadAndUnPackClientServerProtoBuf(r io.Reader) (*protocol.ClientServerCmd, []byte, error) {
	tmp := make([]byte, 4)
	_, err := io.ReadFull(r, tmp) //读取包头长度
	if err != nil {
		//myLogger.Logger.Print(err)
		return nil, nil, err
	}
	len := int32(binary.BigEndian.Uint32(tmp))
	//myLogger.Logger.Printf("HeaderLen %d ", len)
	tmp = make([]byte, len)
	_, err = io.ReadFull(r, tmp) //读取包头
	if err != nil {
		//myLogger.Logger.Print(err)
		return nil, nil, err
	}

	clientServerHeader := &protocol.ClientServerHeader{}
	err = proto.Unmarshal(tmp, clientServerHeader) //得到包头
	if err != nil {
		//myLogger.Logger.PrintError("Unmarshal error %s", err)
		return nil, nil, err
	} else {
		myLogger.Logger.Printf("receive ClientServerHeader: %s", clientServerHeader)
	}

	body := make([]byte, clientServerHeader.MsgLen) //消息体
	_, err = io.ReadFull(r, body)                   //读取消息体
	if err != nil {
		//myLogger.Logger.Print(err)
		return nil, nil, err
	}
	return &clientServerHeader.Cmd, body, nil
}

//func convert2(data []byte) (error) {
//
//
//}
//func UnPackClientServer(r io.Reader) ( interface{}, error) {
//	tmp := make([]byte, 4)
//	_, err := io.ReadFull(r, tmp) //读取长度
//	if err != nil {
//		if err == io.EOF {
//			myLogger.Logger.Print("EOF")
//		} else {
//			myLogger.Logger.Print(err)
//		}
//		return nil, err
//	}
//	len := int32(binary.BigEndian.Uint32(tmp))
//	myLogger.Logger.Printf("readLen %d ", len)
//	requestData := make([]byte, len)
//	_, err = io.ReadFull(r, requestData) //读取内容
//	if err != nil {
//		if err == io.EOF {
//			myLogger.Logger.Print("EOF")
//		} else {
//			myLogger.Logger.Print(err)
//		}
//		return nil, err
//	}
//	client2ServerData := &protocol.Client2Server{}
//	err = proto.Unmarshal(requestData, client2ServerData)
//	if err != nil {
//		return nil, err
//		myLogger.Logger.PrintError("Unmarshal error %s", err)
//	}else{
//		myLogger.Logger.Printf("receive client2ServerData: %s", client2ServerData)
//	}
//	return client2ServerData, nil
//}
