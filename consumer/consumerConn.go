package consumer

import (
	"../mylib/myLogger"
	"../mylib/protocalFuc"
	"../protocol"
	"bufio"
	"errors"
	"github.com/golang/protobuf/proto"
	"io"
	"net"
	"sync"
	"time"
)

const commitTime = 200 * time.Millisecond

type consumerConn struct {
	addr     string
	consumer *Consumer
	conn     net.Conn
	reader   *bufio.Reader
	//writerLock sync.RWMutex
	writer                  *bufio.Writer
	writeMsgAckChan         chan *protocol.PushMsgRsp
	writePriorityMsgAckChan chan *protocol.PushMsgRsp
	writeChan               chan []byte
	exitChan                chan string
}

func NewConn(addr string, consumer *Consumer) (*consumerConn, error) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}
	c := &consumerConn{
		addr:                    addr,
		conn:                    conn,
		reader:                  bufio.NewReader(conn),
		writer:                  bufio.NewWriter(conn),
		writeChan:               make(chan []byte),
		exitChan:                make(chan string),
		writeMsgAckChan:         make(chan *protocol.PushMsgRsp),
		writePriorityMsgAckChan: make(chan *protocol.PushMsgRsp),
		consumer:                consumer,
	}

	return c, nil
}

//连接处理函数
func (c *consumerConn) Handle() {
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		c.readLoop()
		wg.Done()
	}()
	go func() {
		c.writeLoop()
		wg.Done()
	}()
	wg.Wait()
	myLogger.Logger.Print("connect end")
	c.exit()
}

//退出连接
func (c *consumerConn) exit() {
	c.consumer.deleteBrokerConn(c)
	c.writer.Flush()
	c.conn.Close()
	close(c.writeChan)
	close(c.exitChan)
}

//负责从broker读消息
func (c *consumerConn) readLoop() {
	for {
		cmd, msgBody, err := protocalFuc.ReadAndUnPackClientServerProtoBuf(c.reader)
		if err != nil {
			if err == io.EOF {
				myLogger.Logger.Print("EOF")
			} else {
				myLogger.Logger.Print(err)
			}
			c.exitChan <- "bye"
			break
		} else {
			myLogger.Logger.Printf("receive cmd: %s data: %s", cmd, msgBody)
		}
		c.consumer.readChan <- &readData{c.addr, cmd, msgBody}
	}
}

//负责写消息到broker
func (c *consumerConn) writeLoop() {
	timeTicker := time.NewTicker(commitTime) //每200m秒触发一次提交最大消费位移
	var maxMsgId int32
	var lastPartitionName string
	var lastGroupName string
	var NeedCommitAck bool

	var priorityLastMsgId int32
	var priorityLastPartitionName string
	var priorityLastGroupName string
	var NeedCommitPriorityAck bool
	for {
		select {
		case <-timeTicker.C:
			if NeedCommitAck { //普通消息需要ack
				NeedCommitAck = false
				rsp := &protocol.PushMsgRsp{
					Ret:           protocol.RetStatus_Successs,
					PartitionName: lastPartitionName,
					GroupName:     lastGroupName,
					MsgId:         maxMsgId,
				}
				data, err := proto.Marshal(rsp)
				if err != nil {
					myLogger.Logger.PrintError("marshaling error", err)
					break
				}
				reqData, err := protocalFuc.PackClientServerProtoBuf(protocol.ClientServerCmd_CmdPushMsgRsp, data)
				if err != nil {
					myLogger.Logger.PrintError("marshaling error", err)
					break
				}
				_, err = c.writer.Write(reqData)
				if err != nil {
					myLogger.Logger.PrintError("writeLoop1:", err)
				}
				myLogger.Logger.Print("commit", rsp)
			}
			if NeedCommitPriorityAck { //优先级消息需要ack
				NeedCommitPriorityAck = false
				rsp := &protocol.PushMsgRsp{
					Ret:           protocol.RetStatus_Successs,
					PartitionName: priorityLastPartitionName,
					GroupName:     priorityLastGroupName,
					MsgId:         priorityLastMsgId,
					MsgPriority:   1,
				}
				data, err := proto.Marshal(rsp)
				if err != nil {
					myLogger.Logger.PrintError("marshaling error", err)
					break
				}
				reqData, err := protocalFuc.PackClientServerProtoBuf(protocol.ClientServerCmd_CmdPushMsgRsp, data)
				if err != nil {
					myLogger.Logger.PrintError("marshaling error", err)
					break
				}
				_, err = c.writer.Write(reqData)
				if err != nil {
					myLogger.Logger.PrintError("writeLoop1:", err)
				}
				myLogger.Logger.Print("commit", rsp)
			}
			err := c.writer.Flush()
			if err != nil {
				myLogger.Logger.PrintError("writeLoop1:", err)
			}
		case msg := <-c.writeMsgAckChan: //普通消息
			if msg.PartitionName == lastPartitionName && msg.GroupName == lastGroupName { //分区及组名都没有改变，只需更新最大id
				NeedCommitAck = true
				maxMsgId = msg.MsgId //先不ack，等一段时间再统一ack
				myLogger.Logger.Print("defer commit ", msg)
			} else { //改变了，需直接将上次的commit上传
				if lastPartitionName == "" {
					lastPartitionName = msg.PartitionName
					lastGroupName = msg.GroupName
					maxMsgId = msg.MsgId
					continue
				}
				NeedCommitAck = true
				rsp := &protocol.PushMsgRsp{
					Ret:           protocol.RetStatus_Successs,
					PartitionName: lastPartitionName,
					GroupName:     lastGroupName,
					MsgId:         maxMsgId,
				}
				lastPartitionName = msg.PartitionName
				lastGroupName = msg.GroupName
				maxMsgId = msg.MsgId
				myLogger.Logger.Print("commit rightnow")
				data, err := proto.Marshal(rsp)
				if err != nil {
					myLogger.Logger.PrintError("marshaling error", err)
					break
				}
				reqData, err := protocalFuc.PackClientServerProtoBuf(protocol.ClientServerCmd_CmdPushMsgRsp, data)
				if err != nil {
					myLogger.Logger.PrintError("marshaling error", err)
					break
				}
				_, err = c.writer.Write(reqData)
				if err != nil {
					myLogger.Logger.PrintError("writeLoop1:", err)
				}
			}

		case msg := <-c.writePriorityMsgAckChan: //优先级消息
			if msg.PartitionName == priorityLastPartitionName && msg.GroupName == priorityLastGroupName { //分区及组名都没有改变，只需更最终Id
				NeedCommitPriorityAck = true
				priorityLastMsgId = msg.MsgId //先不ack，等一段时间再统一ack
				myLogger.Logger.Print("defer commit Priority msg: ", msg)
			} else { //改变了，需直接将上次的commit上传
				if priorityLastPartitionName == "" {
					priorityLastPartitionName = msg.PartitionName
					priorityLastGroupName = msg.GroupName
					priorityLastMsgId = msg.MsgId
					continue
				}
				NeedCommitPriorityAck = true
				rsp := &protocol.PushMsgRsp{
					Ret:           protocol.RetStatus_Successs,
					PartitionName: priorityLastPartitionName,
					GroupName:     priorityLastGroupName,
					MsgId:         priorityLastMsgId,
					MsgPriority:   1,
				}
				priorityLastPartitionName = msg.PartitionName
				priorityLastGroupName = msg.GroupName
				priorityLastMsgId = msg.MsgId
				myLogger.Logger.Print("commit Priority msg: rightnow")
				data, err := proto.Marshal(rsp)
				if err != nil {
					myLogger.Logger.PrintError("marshaling error", err)
					break
				}
				reqData, err := protocalFuc.PackClientServerProtoBuf(protocol.ClientServerCmd_CmdPushMsgRsp, data)
				if err != nil {
					myLogger.Logger.PrintError("marshaling error", err)
					break
				}
				_, err = c.writer.Write(reqData)
				if err != nil {
					myLogger.Logger.PrintError("writeLoop1:", err)
				}
			}
		case request := <-c.writeChan:
			myLogger.Logger.Printf("writeLoop1: %s", request)
			_, err := c.writer.Write(request)
			if err != nil {
				myLogger.Logger.PrintError("writeLoop1:", err)
			}
			//err = c.writer.Flush()
			//if err != nil {
			//	myLogger.Logger.PrintError(err)
			//}
			myLogger.Logger.Printf("writeLoop2: %s", request)
		case <-c.exitChan:
			goto exit

		}
	}
exit:
	timeTicker.Stop()
	myLogger.Logger.Printf("writeLoop exit:")
}

//往连接中放数据
func (c *consumerConn) Put(data []byte) error {

	select {
	case c.writeChan <- data:
		//myLogger.Logger.Print("do not have client")
	case <-time.After(3000 * time.Microsecond):
		myLogger.Logger.PrintError("write fail")
		return errors.New("write fail")
	}

	return nil

}

//往连接中放数据
func (c *consumerConn) PutMsg(msg *protocol.PushMsgRsp) error {

	select {
	case c.writeMsgAckChan <- msg:
		//myLogger.Logger.Print("do not have client")
	case <-time.After(3000 * time.Microsecond):
		myLogger.Logger.PrintError("write fail")
		return errors.New("write fail")
	}

	return nil

}
