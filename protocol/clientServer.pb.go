// Code generated by protoc-gen-go.
// source: clientServer.proto
// DO NOT EDIT!

package protocol

import proto "github.com/golang/protobuf/proto"
import fmt "fmt"
import math "math"

// Reference imports to suppress errors if they are not otherwise used.
var _ = proto.Marshal
var _ = fmt.Errorf
var _ = math.Inf

type ClientServerCmd int32

const (
	ClientServerCmd_CmdCreatTopicReq              ClientServerCmd = 0
	ClientServerCmd_CmdCreatTopicRsp              ClientServerCmd = 1
	ClientServerCmd_CmdDeleteTopicReq             ClientServerCmd = 2
	ClientServerCmd_CmdDeleteTopicRsp             ClientServerCmd = 3
	ClientServerCmd_CmdGetPublisherPartitionReq   ClientServerCmd = 4
	ClientServerCmd_CmdGetPublisherPartitionRsp   ClientServerCmd = 5
	ClientServerCmd_CmdPublishReq                 ClientServerCmd = 6
	ClientServerCmd_CmdPublishRsp                 ClientServerCmd = 7
	ClientServerCmd_CmdGetConsumerPartitionReq    ClientServerCmd = 8
	ClientServerCmd_CmdGetConsumerPartitionRsp    ClientServerCmd = 9
	ClientServerCmd_CmdSubscribePartitionReq      ClientServerCmd = 10
	ClientServerCmd_CmdSubscribePartitionRsp      ClientServerCmd = 11
	ClientServerCmd_CmdSubscribeTopicReq          ClientServerCmd = 12
	ClientServerCmd_CmdSubscribeTopicRsp          ClientServerCmd = 13
	ClientServerCmd_CmdRegisterConsumerReq        ClientServerCmd = 14
	ClientServerCmd_CmdRegisterConsumerRsp        ClientServerCmd = 15
	ClientServerCmd_CmdUnRegisterConsumerReq      ClientServerCmd = 16
	ClientServerCmd_CmdUnRegisterConsumerRsp      ClientServerCmd = 17
	ClientServerCmd_CmdCommitReadyNumReq          ClientServerCmd = 20
	ClientServerCmd_CmdCommitReadyNumRsp          ClientServerCmd = 21
	ClientServerCmd_CmdChangeConsumerPartitionReq ClientServerCmd = 22
	ClientServerCmd_CmdChangeConsumerPartitionRsp ClientServerCmd = 23
	ClientServerCmd_CmdPushMsgReq                 ClientServerCmd = 24
	ClientServerCmd_CmdPushMsgRsp                 ClientServerCmd = 25
	ClientServerCmd_CmdPublishWithoutAskReq       ClientServerCmd = 26
	ClientServerCmd_CmdPublishWithoutAskRsp       ClientServerCmd = 27
)

var ClientServerCmd_name = map[int32]string{
	0:  "CmdCreatTopicReq",
	1:  "CmdCreatTopicRsp",
	2:  "CmdDeleteTopicReq",
	3:  "CmdDeleteTopicRsp",
	4:  "CmdGetPublisherPartitionReq",
	5:  "CmdGetPublisherPartitionRsp",
	6:  "CmdPublishReq",
	7:  "CmdPublishRsp",
	8:  "CmdGetConsumerPartitionReq",
	9:  "CmdGetConsumerPartitionRsp",
	10: "CmdSubscribePartitionReq",
	11: "CmdSubscribePartitionRsp",
	12: "CmdSubscribeTopicReq",
	13: "CmdSubscribeTopicRsp",
	14: "CmdRegisterConsumerReq",
	15: "CmdRegisterConsumerRsp",
	16: "CmdUnRegisterConsumerReq",
	17: "CmdUnRegisterConsumerRsp",
	20: "CmdCommitReadyNumReq",
	21: "CmdCommitReadyNumRsp",
	22: "CmdChangeConsumerPartitionReq",
	23: "CmdChangeConsumerPartitionRsp",
	24: "CmdPushMsgReq",
	25: "CmdPushMsgRsp",
	26: "CmdPublishWithoutAskReq",
	27: "CmdPublishWithoutAskRsp",
}
var ClientServerCmd_value = map[string]int32{
	"CmdCreatTopicReq":              0,
	"CmdCreatTopicRsp":              1,
	"CmdDeleteTopicReq":             2,
	"CmdDeleteTopicRsp":             3,
	"CmdGetPublisherPartitionReq":   4,
	"CmdGetPublisherPartitionRsp":   5,
	"CmdPublishReq":                 6,
	"CmdPublishRsp":                 7,
	"CmdGetConsumerPartitionReq":    8,
	"CmdGetConsumerPartitionRsp":    9,
	"CmdSubscribePartitionReq":      10,
	"CmdSubscribePartitionRsp":      11,
	"CmdSubscribeTopicReq":          12,
	"CmdSubscribeTopicRsp":          13,
	"CmdRegisterConsumerReq":        14,
	"CmdRegisterConsumerRsp":        15,
	"CmdUnRegisterConsumerReq":      16,
	"CmdUnRegisterConsumerRsp":      17,
	"CmdCommitReadyNumReq":          20,
	"CmdCommitReadyNumRsp":          21,
	"CmdChangeConsumerPartitionReq": 22,
	"CmdChangeConsumerPartitionRsp": 23,
	"CmdPushMsgReq":                 24,
	"CmdPushMsgRsp":                 25,
	"CmdPublishWithoutAskReq":       26,
	"CmdPublishWithoutAskRsp":       27,
}

func (x ClientServerCmd) String() string {
	return proto.EnumName(ClientServerCmd_name, int32(x))
}
func (ClientServerCmd) EnumDescriptor() ([]byte, []int) { return fileDescriptor1, []int{0} }

// broker与 Client之间的数据
type ClientServerHeader struct {
	Cmd    ClientServerCmd `protobuf:"varint,1,opt,name=cmd,enum=protocol.ClientServerCmd" json:"cmd,omitempty"`
	MsgLen int32           `protobuf:"varint,2,opt,name=msgLen" json:"msgLen,omitempty"`
}

func (m *ClientServerHeader) Reset()                    { *m = ClientServerHeader{} }
func (m *ClientServerHeader) String() string            { return proto.CompactTextString(m) }
func (*ClientServerHeader) ProtoMessage()               {}
func (*ClientServerHeader) Descriptor() ([]byte, []int) { return fileDescriptor1, []int{0} }

func (m *ClientServerHeader) GetCmd() ClientServerCmd {
	if m != nil {
		return m.Cmd
	}
	return ClientServerCmd_CmdCreatTopicReq
}

func (m *ClientServerHeader) GetMsgLen() int32 {
	if m != nil {
		return m.MsgLen
	}
	return 0
}

type CreatTopicReq struct {
	TopicName    string `protobuf:"bytes,1,opt,name=TopicName" json:"TopicName,omitempty"`
	PartitionNum int32  `protobuf:"varint,2,opt,name=PartitionNum" json:"PartitionNum,omitempty"`
}

func (m *CreatTopicReq) Reset()                    { *m = CreatTopicReq{} }
func (m *CreatTopicReq) String() string            { return proto.CompactTextString(m) }
func (*CreatTopicReq) ProtoMessage()               {}
func (*CreatTopicReq) Descriptor() ([]byte, []int) { return fileDescriptor1, []int{1} }

func (m *CreatTopicReq) GetTopicName() string {
	if m != nil {
		return m.TopicName
	}
	return ""
}

func (m *CreatTopicReq) GetPartitionNum() int32 {
	if m != nil {
		return m.PartitionNum
	}
	return 0
}

type CreatTopicRsp struct {
	Ret        RetStatus    `protobuf:"varint,1,opt,name=ret,enum=protocol.RetStatus" json:"ret,omitempty"`
	Partitions []*Partition `protobuf:"bytes,2,rep,name=Partitions" json:"Partitions,omitempty"`
}

func (m *CreatTopicRsp) Reset()                    { *m = CreatTopicRsp{} }
func (m *CreatTopicRsp) String() string            { return proto.CompactTextString(m) }
func (*CreatTopicRsp) ProtoMessage()               {}
func (*CreatTopicRsp) Descriptor() ([]byte, []int) { return fileDescriptor1, []int{2} }

func (m *CreatTopicRsp) GetRet() RetStatus {
	if m != nil {
		return m.Ret
	}
	return RetStatus_Successs
}

func (m *CreatTopicRsp) GetPartitions() []*Partition {
	if m != nil {
		return m.Partitions
	}
	return nil
}

type DeleteTopicReq struct {
	TopicName string `protobuf:"bytes,1,opt,name=TopicName" json:"TopicName,omitempty"`
}

func (m *DeleteTopicReq) Reset()                    { *m = DeleteTopicReq{} }
func (m *DeleteTopicReq) String() string            { return proto.CompactTextString(m) }
func (*DeleteTopicReq) ProtoMessage()               {}
func (*DeleteTopicReq) Descriptor() ([]byte, []int) { return fileDescriptor1, []int{3} }

func (m *DeleteTopicReq) GetTopicName() string {
	if m != nil {
		return m.TopicName
	}
	return ""
}

type DeleteTopicRsp struct {
	Ret RetStatus `protobuf:"varint,1,opt,name=ret,enum=protocol.RetStatus" json:"ret,omitempty"`
}

func (m *DeleteTopicRsp) Reset()                    { *m = DeleteTopicRsp{} }
func (m *DeleteTopicRsp) String() string            { return proto.CompactTextString(m) }
func (*DeleteTopicRsp) ProtoMessage()               {}
func (*DeleteTopicRsp) Descriptor() ([]byte, []int) { return fileDescriptor1, []int{4} }

func (m *DeleteTopicRsp) GetRet() RetStatus {
	if m != nil {
		return m.Ret
	}
	return RetStatus_Successs
}

type PublishReq struct {
	PartitionName string   `protobuf:"bytes,1,opt,name=PartitionName" json:"PartitionName,omitempty"`
	Msg           *Message `protobuf:"bytes,2,opt,name=msg" json:"msg,omitempty"`
}

func (m *PublishReq) Reset()                    { *m = PublishReq{} }
func (m *PublishReq) String() string            { return proto.CompactTextString(m) }
func (*PublishReq) ProtoMessage()               {}
func (*PublishReq) Descriptor() ([]byte, []int) { return fileDescriptor1, []int{5} }

func (m *PublishReq) GetPartitionName() string {
	if m != nil {
		return m.PartitionName
	}
	return ""
}

func (m *PublishReq) GetMsg() *Message {
	if m != nil {
		return m.Msg
	}
	return nil
}

type PublishRsp struct {
	Ret RetStatus `protobuf:"varint,1,opt,name=ret,enum=protocol.RetStatus" json:"ret,omitempty"`
}

func (m *PublishRsp) Reset()                    { *m = PublishRsp{} }
func (m *PublishRsp) String() string            { return proto.CompactTextString(m) }
func (*PublishRsp) ProtoMessage()               {}
func (*PublishRsp) Descriptor() ([]byte, []int) { return fileDescriptor1, []int{6} }

func (m *PublishRsp) GetRet() RetStatus {
	if m != nil {
		return m.Ret
	}
	return RetStatus_Successs
}

type GetPublisherPartitionReq struct {
	TopicName string `protobuf:"bytes,1,opt,name=TopicName" json:"TopicName,omitempty"`
}

func (m *GetPublisherPartitionReq) Reset()                    { *m = GetPublisherPartitionReq{} }
func (m *GetPublisherPartitionReq) String() string            { return proto.CompactTextString(m) }
func (*GetPublisherPartitionReq) ProtoMessage()               {}
func (*GetPublisherPartitionReq) Descriptor() ([]byte, []int) { return fileDescriptor1, []int{7} }

func (m *GetPublisherPartitionReq) GetTopicName() string {
	if m != nil {
		return m.TopicName
	}
	return ""
}

type GetPublisherPartitionRsp struct {
	Ret        RetStatus    `protobuf:"varint,1,opt,name=ret,enum=protocol.RetStatus" json:"ret,omitempty"`
	Partitions []*Partition `protobuf:"bytes,2,rep,name=Partitions" json:"Partitions,omitempty"`
}

func (m *GetPublisherPartitionRsp) Reset()                    { *m = GetPublisherPartitionRsp{} }
func (m *GetPublisherPartitionRsp) String() string            { return proto.CompactTextString(m) }
func (*GetPublisherPartitionRsp) ProtoMessage()               {}
func (*GetPublisherPartitionRsp) Descriptor() ([]byte, []int) { return fileDescriptor1, []int{8} }

func (m *GetPublisherPartitionRsp) GetRet() RetStatus {
	if m != nil {
		return m.Ret
	}
	return RetStatus_Successs
}

func (m *GetPublisherPartitionRsp) GetPartitions() []*Partition {
	if m != nil {
		return m.Partitions
	}
	return nil
}

type GetConsumerPartitionReq struct {
	TopicName string `protobuf:"bytes,1,opt,name=TopicName" json:"TopicName,omitempty"`
	GroupName string `protobuf:"bytes,2,opt,name=GroupName" json:"GroupName,omitempty"`
}

func (m *GetConsumerPartitionReq) Reset()                    { *m = GetConsumerPartitionReq{} }
func (m *GetConsumerPartitionReq) String() string            { return proto.CompactTextString(m) }
func (*GetConsumerPartitionReq) ProtoMessage()               {}
func (*GetConsumerPartitionReq) Descriptor() ([]byte, []int) { return fileDescriptor1, []int{9} }

func (m *GetConsumerPartitionReq) GetTopicName() string {
	if m != nil {
		return m.TopicName
	}
	return ""
}

func (m *GetConsumerPartitionReq) GetGroupName() string {
	if m != nil {
		return m.GroupName
	}
	return ""
}

type GetConsumerPartitionRsp struct {
	Ret        RetStatus    `protobuf:"varint,1,opt,name=ret,enum=protocol.RetStatus" json:"ret,omitempty"`
	Partitions []*Partition `protobuf:"bytes,2,rep,name=Partitions" json:"Partitions,omitempty"`
}

func (m *GetConsumerPartitionRsp) Reset()                    { *m = GetConsumerPartitionRsp{} }
func (m *GetConsumerPartitionRsp) String() string            { return proto.CompactTextString(m) }
func (*GetConsumerPartitionRsp) ProtoMessage()               {}
func (*GetConsumerPartitionRsp) Descriptor() ([]byte, []int) { return fileDescriptor1, []int{10} }

func (m *GetConsumerPartitionRsp) GetRet() RetStatus {
	if m != nil {
		return m.Ret
	}
	return RetStatus_Successs
}

func (m *GetConsumerPartitionRsp) GetPartitions() []*Partition {
	if m != nil {
		return m.Partitions
	}
	return nil
}

type SubscribePartitionReq struct {
	PartitionName string `protobuf:"bytes,1,opt,name=PartitionName" json:"PartitionName,omitempty"`
	GroupName     string `protobuf:"bytes,2,opt,name=GroupName" json:"GroupName,omitempty"`
	RebalanceId   int32  `protobuf:"varint,3,opt,name=RebalanceId" json:"RebalanceId,omitempty"`
}

func (m *SubscribePartitionReq) Reset()                    { *m = SubscribePartitionReq{} }
func (m *SubscribePartitionReq) String() string            { return proto.CompactTextString(m) }
func (*SubscribePartitionReq) ProtoMessage()               {}
func (*SubscribePartitionReq) Descriptor() ([]byte, []int) { return fileDescriptor1, []int{11} }

func (m *SubscribePartitionReq) GetPartitionName() string {
	if m != nil {
		return m.PartitionName
	}
	return ""
}

func (m *SubscribePartitionReq) GetGroupName() string {
	if m != nil {
		return m.GroupName
	}
	return ""
}

func (m *SubscribePartitionReq) GetRebalanceId() int32 {
	if m != nil {
		return m.RebalanceId
	}
	return 0
}

type SubscribePartitionRsp struct {
	Ret RetStatus `protobuf:"varint,1,opt,name=ret,enum=protocol.RetStatus" json:"ret,omitempty"`
}

func (m *SubscribePartitionRsp) Reset()                    { *m = SubscribePartitionRsp{} }
func (m *SubscribePartitionRsp) String() string            { return proto.CompactTextString(m) }
func (*SubscribePartitionRsp) ProtoMessage()               {}
func (*SubscribePartitionRsp) Descriptor() ([]byte, []int) { return fileDescriptor1, []int{12} }

func (m *SubscribePartitionRsp) GetRet() RetStatus {
	if m != nil {
		return m.Ret
	}
	return RetStatus_Successs
}

type SubscribeTopicReq struct {
	GroupName string `protobuf:"bytes,1,opt,name=GroupName" json:"GroupName,omitempty"`
	TopicName string `protobuf:"bytes,2,opt,name=TopicName" json:"TopicName,omitempty"`
}

func (m *SubscribeTopicReq) Reset()                    { *m = SubscribeTopicReq{} }
func (m *SubscribeTopicReq) String() string            { return proto.CompactTextString(m) }
func (*SubscribeTopicReq) ProtoMessage()               {}
func (*SubscribeTopicReq) Descriptor() ([]byte, []int) { return fileDescriptor1, []int{13} }

func (m *SubscribeTopicReq) GetGroupName() string {
	if m != nil {
		return m.GroupName
	}
	return ""
}

func (m *SubscribeTopicReq) GetTopicName() string {
	if m != nil {
		return m.TopicName
	}
	return ""
}

type SubscribeTopicRsp struct {
	Ret RetStatus `protobuf:"varint,1,opt,name=ret,enum=protocol.RetStatus" json:"ret,omitempty"`
}

func (m *SubscribeTopicRsp) Reset()                    { *m = SubscribeTopicRsp{} }
func (m *SubscribeTopicRsp) String() string            { return proto.CompactTextString(m) }
func (*SubscribeTopicRsp) ProtoMessage()               {}
func (*SubscribeTopicRsp) Descriptor() ([]byte, []int) { return fileDescriptor1, []int{14} }

func (m *SubscribeTopicRsp) GetRet() RetStatus {
	if m != nil {
		return m.Ret
	}
	return RetStatus_Successs
}

type RegisterConsumerReq struct {
	GroupName string `protobuf:"bytes,1,opt,name=GroupName" json:"GroupName,omitempty"`
}

func (m *RegisterConsumerReq) Reset()                    { *m = RegisterConsumerReq{} }
func (m *RegisterConsumerReq) String() string            { return proto.CompactTextString(m) }
func (*RegisterConsumerReq) ProtoMessage()               {}
func (*RegisterConsumerReq) Descriptor() ([]byte, []int) { return fileDescriptor1, []int{15} }

func (m *RegisterConsumerReq) GetGroupName() string {
	if m != nil {
		return m.GroupName
	}
	return ""
}

type RegisterConsumerRsp struct {
	Ret RetStatus `protobuf:"varint,1,opt,name=ret,enum=protocol.RetStatus" json:"ret,omitempty"`
}

func (m *RegisterConsumerRsp) Reset()                    { *m = RegisterConsumerRsp{} }
func (m *RegisterConsumerRsp) String() string            { return proto.CompactTextString(m) }
func (*RegisterConsumerRsp) ProtoMessage()               {}
func (*RegisterConsumerRsp) Descriptor() ([]byte, []int) { return fileDescriptor1, []int{16} }

func (m *RegisterConsumerRsp) GetRet() RetStatus {
	if m != nil {
		return m.Ret
	}
	return RetStatus_Successs
}

type UnRegisterConsumerReq struct {
	GroupName string `protobuf:"bytes,1,opt,name=GroupName" json:"GroupName,omitempty"`
}

func (m *UnRegisterConsumerReq) Reset()                    { *m = UnRegisterConsumerReq{} }
func (m *UnRegisterConsumerReq) String() string            { return proto.CompactTextString(m) }
func (*UnRegisterConsumerReq) ProtoMessage()               {}
func (*UnRegisterConsumerReq) Descriptor() ([]byte, []int) { return fileDescriptor1, []int{17} }

func (m *UnRegisterConsumerReq) GetGroupName() string {
	if m != nil {
		return m.GroupName
	}
	return ""
}

type UnRegisterConsumerRsp struct {
	Ret RetStatus `protobuf:"varint,1,opt,name=ret,enum=protocol.RetStatus" json:"ret,omitempty"`
}

func (m *UnRegisterConsumerRsp) Reset()                    { *m = UnRegisterConsumerRsp{} }
func (m *UnRegisterConsumerRsp) String() string            { return proto.CompactTextString(m) }
func (*UnRegisterConsumerRsp) ProtoMessage()               {}
func (*UnRegisterConsumerRsp) Descriptor() ([]byte, []int) { return fileDescriptor1, []int{18} }

func (m *UnRegisterConsumerRsp) GetRet() RetStatus {
	if m != nil {
		return m.Ret
	}
	return RetStatus_Successs
}

type CommitReadyNumReq struct {
	ReadyNum int32 `protobuf:"varint,1,opt,name=ReadyNum" json:"ReadyNum,omitempty"`
}

func (m *CommitReadyNumReq) Reset()                    { *m = CommitReadyNumReq{} }
func (m *CommitReadyNumReq) String() string            { return proto.CompactTextString(m) }
func (*CommitReadyNumReq) ProtoMessage()               {}
func (*CommitReadyNumReq) Descriptor() ([]byte, []int) { return fileDescriptor1, []int{19} }

func (m *CommitReadyNumReq) GetReadyNum() int32 {
	if m != nil {
		return m.ReadyNum
	}
	return 0
}

type CommitReadyNumReqRsp struct {
	Ret RetStatus `protobuf:"varint,1,opt,name=ret,enum=protocol.RetStatus" json:"ret,omitempty"`
}

func (m *CommitReadyNumReqRsp) Reset()                    { *m = CommitReadyNumReqRsp{} }
func (m *CommitReadyNumReqRsp) String() string            { return proto.CompactTextString(m) }
func (*CommitReadyNumReqRsp) ProtoMessage()               {}
func (*CommitReadyNumReqRsp) Descriptor() ([]byte, []int) { return fileDescriptor1, []int{20} }

func (m *CommitReadyNumReqRsp) GetRet() RetStatus {
	if m != nil {
		return m.Ret
	}
	return RetStatus_Successs
}

type ChangeConsumerPartitionReq struct {
	Partitions  []*Partition `protobuf:"bytes,1,rep,name=Partitions" json:"Partitions,omitempty"`
	RebalanceId int32        `protobuf:"varint,2,opt,name=RebalanceId" json:"RebalanceId,omitempty"`
}

func (m *ChangeConsumerPartitionReq) Reset()                    { *m = ChangeConsumerPartitionReq{} }
func (m *ChangeConsumerPartitionReq) String() string            { return proto.CompactTextString(m) }
func (*ChangeConsumerPartitionReq) ProtoMessage()               {}
func (*ChangeConsumerPartitionReq) Descriptor() ([]byte, []int) { return fileDescriptor1, []int{21} }

func (m *ChangeConsumerPartitionReq) GetPartitions() []*Partition {
	if m != nil {
		return m.Partitions
	}
	return nil
}

func (m *ChangeConsumerPartitionReq) GetRebalanceId() int32 {
	if m != nil {
		return m.RebalanceId
	}
	return 0
}

type ChangeConsumerPartitionRsp struct {
	Ret RetStatus `protobuf:"varint,1,opt,name=ret,enum=protocol.RetStatus" json:"ret,omitempty"`
}

func (m *ChangeConsumerPartitionRsp) Reset()                    { *m = ChangeConsumerPartitionRsp{} }
func (m *ChangeConsumerPartitionRsp) String() string            { return proto.CompactTextString(m) }
func (*ChangeConsumerPartitionRsp) ProtoMessage()               {}
func (*ChangeConsumerPartitionRsp) Descriptor() ([]byte, []int) { return fileDescriptor1, []int{22} }

func (m *ChangeConsumerPartitionRsp) GetRet() RetStatus {
	if m != nil {
		return m.Ret
	}
	return RetStatus_Successs
}

type PushMsgReq struct {
	PartitionName string   `protobuf:"bytes,1,opt,name=PartitionName" json:"PartitionName,omitempty"`
	Msg           *Message `protobuf:"bytes,2,opt,name=msg" json:"msg,omitempty"`
	GroupName     string   `protobuf:"bytes,3,opt,name=GroupName" json:"GroupName,omitempty"`
}

func (m *PushMsgReq) Reset()                    { *m = PushMsgReq{} }
func (m *PushMsgReq) String() string            { return proto.CompactTextString(m) }
func (*PushMsgReq) ProtoMessage()               {}
func (*PushMsgReq) Descriptor() ([]byte, []int) { return fileDescriptor1, []int{23} }

func (m *PushMsgReq) GetPartitionName() string {
	if m != nil {
		return m.PartitionName
	}
	return ""
}

func (m *PushMsgReq) GetMsg() *Message {
	if m != nil {
		return m.Msg
	}
	return nil
}

func (m *PushMsgReq) GetGroupName() string {
	if m != nil {
		return m.GroupName
	}
	return ""
}

type PushMsgRsp struct {
	Ret           RetStatus `protobuf:"varint,1,opt,name=ret,enum=protocol.RetStatus" json:"ret,omitempty"`
	PartitionName string    `protobuf:"bytes,2,opt,name=PartitionName" json:"PartitionName,omitempty"`
	MsgId         int32     `protobuf:"varint,3,opt,name=MsgId" json:"MsgId,omitempty"`
	GroupName     string    `protobuf:"bytes,4,opt,name=GroupName" json:"GroupName,omitempty"`
}

func (m *PushMsgRsp) Reset()                    { *m = PushMsgRsp{} }
func (m *PushMsgRsp) String() string            { return proto.CompactTextString(m) }
func (*PushMsgRsp) ProtoMessage()               {}
func (*PushMsgRsp) Descriptor() ([]byte, []int) { return fileDescriptor1, []int{24} }

func (m *PushMsgRsp) GetRet() RetStatus {
	if m != nil {
		return m.Ret
	}
	return RetStatus_Successs
}

func (m *PushMsgRsp) GetPartitionName() string {
	if m != nil {
		return m.PartitionName
	}
	return ""
}

func (m *PushMsgRsp) GetMsgId() int32 {
	if m != nil {
		return m.MsgId
	}
	return 0
}

func (m *PushMsgRsp) GetGroupName() string {
	if m != nil {
		return m.GroupName
	}
	return ""
}

type PublishWithoutAskReq struct {
	PartitionName string   `protobuf:"bytes,1,opt,name=PartitionName" json:"PartitionName,omitempty"`
	Msg           *Message `protobuf:"bytes,2,opt,name=msg" json:"msg,omitempty"`
}

func (m *PublishWithoutAskReq) Reset()                    { *m = PublishWithoutAskReq{} }
func (m *PublishWithoutAskReq) String() string            { return proto.CompactTextString(m) }
func (*PublishWithoutAskReq) ProtoMessage()               {}
func (*PublishWithoutAskReq) Descriptor() ([]byte, []int) { return fileDescriptor1, []int{25} }

func (m *PublishWithoutAskReq) GetPartitionName() string {
	if m != nil {
		return m.PartitionName
	}
	return ""
}

func (m *PublishWithoutAskReq) GetMsg() *Message {
	if m != nil {
		return m.Msg
	}
	return nil
}

type PublishWithoutAskResp struct {
	Ret RetStatus `protobuf:"varint,1,opt,name=ret,enum=protocol.RetStatus" json:"ret,omitempty"`
}

func (m *PublishWithoutAskResp) Reset()                    { *m = PublishWithoutAskResp{} }
func (m *PublishWithoutAskResp) String() string            { return proto.CompactTextString(m) }
func (*PublishWithoutAskResp) ProtoMessage()               {}
func (*PublishWithoutAskResp) Descriptor() ([]byte, []int) { return fileDescriptor1, []int{26} }

func (m *PublishWithoutAskResp) GetRet() RetStatus {
	if m != nil {
		return m.Ret
	}
	return RetStatus_Successs
}

func init() {
	proto.RegisterType((*ClientServerHeader)(nil), "protocol.ClientServerHeader")
	proto.RegisterType((*CreatTopicReq)(nil), "protocol.CreatTopicReq")
	proto.RegisterType((*CreatTopicRsp)(nil), "protocol.CreatTopicRsp")
	proto.RegisterType((*DeleteTopicReq)(nil), "protocol.DeleteTopicReq")
	proto.RegisterType((*DeleteTopicRsp)(nil), "protocol.DeleteTopicRsp")
	proto.RegisterType((*PublishReq)(nil), "protocol.PublishReq")
	proto.RegisterType((*PublishRsp)(nil), "protocol.PublishRsp")
	proto.RegisterType((*GetPublisherPartitionReq)(nil), "protocol.GetPublisherPartitionReq")
	proto.RegisterType((*GetPublisherPartitionRsp)(nil), "protocol.GetPublisherPartitionRsp")
	proto.RegisterType((*GetConsumerPartitionReq)(nil), "protocol.GetConsumerPartitionReq")
	proto.RegisterType((*GetConsumerPartitionRsp)(nil), "protocol.GetConsumerPartitionRsp")
	proto.RegisterType((*SubscribePartitionReq)(nil), "protocol.SubscribePartitionReq")
	proto.RegisterType((*SubscribePartitionRsp)(nil), "protocol.SubscribePartitionRsp")
	proto.RegisterType((*SubscribeTopicReq)(nil), "protocol.SubscribeTopicReq")
	proto.RegisterType((*SubscribeTopicRsp)(nil), "protocol.SubscribeTopicRsp")
	proto.RegisterType((*RegisterConsumerReq)(nil), "protocol.RegisterConsumerReq")
	proto.RegisterType((*RegisterConsumerRsp)(nil), "protocol.RegisterConsumerRsp")
	proto.RegisterType((*UnRegisterConsumerReq)(nil), "protocol.UnRegisterConsumerReq")
	proto.RegisterType((*UnRegisterConsumerRsp)(nil), "protocol.UnRegisterConsumerRsp")
	proto.RegisterType((*CommitReadyNumReq)(nil), "protocol.CommitReadyNumReq")
	proto.RegisterType((*CommitReadyNumReqRsp)(nil), "protocol.CommitReadyNumReqRsp")
	proto.RegisterType((*ChangeConsumerPartitionReq)(nil), "protocol.ChangeConsumerPartitionReq")
	proto.RegisterType((*ChangeConsumerPartitionRsp)(nil), "protocol.ChangeConsumerPartitionRsp")
	proto.RegisterType((*PushMsgReq)(nil), "protocol.PushMsgReq")
	proto.RegisterType((*PushMsgRsp)(nil), "protocol.PushMsgRsp")
	proto.RegisterType((*PublishWithoutAskReq)(nil), "protocol.PublishWithoutAskReq")
	proto.RegisterType((*PublishWithoutAskResp)(nil), "protocol.PublishWithoutAskResp")
	proto.RegisterEnum("protocol.ClientServerCmd", ClientServerCmd_name, ClientServerCmd_value)
}

func init() { proto.RegisterFile("clientServer.proto", fileDescriptor1) }

var fileDescriptor1 = []byte{
	// 755 bytes of a gzipped FileDescriptorProto
	0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0xff, 0xb4, 0x56, 0x6d, 0x4f, 0xd3, 0x5e,
	0x14, 0xff, 0x6f, 0x63, 0xfc, 0xe1, 0x8c, 0xc1, 0xdd, 0x65, 0x83, 0x32, 0x50, 0x67, 0xd5, 0x84,
	0x68, 0x32, 0x13, 0x16, 0xa3, 0x31, 0x4a, 0x62, 0x6a, 0x82, 0x26, 0x82, 0x58, 0x24, 0xc4, 0x97,
	0xdd, 0x7a, 0xb3, 0x35, 0xec, 0xae, 0xd7, 0x9e, 0x5b, 0x12, 0x5f, 0xf8, 0x11, 0xfc, 0x70, 0x7e,
	0x23, 0xd3, 0xae, 0xeb, 0x73, 0xa1, 0x24, 0xf0, 0x6a, 0xbb, 0xe7, 0x77, 0x1e, 0x7e, 0xe7, 0x9e,
	0x87, 0x5b, 0xa0, 0xa3, 0xa9, 0xc5, 0x66, 0xf2, 0x8c, 0x39, 0x57, 0xcc, 0xe9, 0x0b, 0xc7, 0x96,
	0x36, 0x5d, 0xf1, 0x7f, 0x46, 0xf6, 0xb4, 0xdb, 0xe4, 0x0c, 0xd1, 0x18, 0xb3, 0x39, 0xa0, 0xfe,
	0x00, 0xaa, 0xc5, 0xd4, 0x3f, 0x31, 0xc3, 0x64, 0x0e, 0x7d, 0x01, 0xb5, 0x11, 0x37, 0x95, 0x4a,
	0xaf, 0xb2, 0xbf, 0x7e, 0xb0, 0xd3, 0x5f, 0x18, 0xf7, 0xe3, 0xaa, 0x1a, 0x37, 0x75, 0x4f, 0x8b,
	0x6e, 0xc1, 0x32, 0xc7, 0xf1, 0x17, 0x36, 0x53, 0xaa, 0xbd, 0xca, 0x7e, 0x5d, 0x0f, 0x4e, 0xea,
	0x37, 0x68, 0x6a, 0x0e, 0x33, 0xe4, 0x77, 0x5b, 0x58, 0x23, 0x9d, 0xfd, 0xa4, 0x7b, 0xb0, 0xea,
	0xff, 0x3f, 0x31, 0x38, 0xf3, 0x7d, 0xaf, 0xea, 0x91, 0x80, 0xaa, 0xb0, 0x76, 0x6a, 0x38, 0xd2,
	0x92, 0x96, 0x3d, 0x3b, 0x71, 0x79, 0xe0, 0x2c, 0x21, 0x53, 0x2f, 0x13, 0x2e, 0x51, 0xd0, 0x67,
	0x50, 0x73, 0x98, 0x0c, 0x88, 0x6e, 0x46, 0x44, 0x75, 0x26, 0xcf, 0xa4, 0x21, 0x5d, 0xd4, 0x3d,
	0x9c, 0x0e, 0x00, 0x42, 0x3f, 0xa8, 0x54, 0x7b, 0xb5, 0xfd, 0x46, 0x5c, 0x3b, 0xc4, 0xf4, 0x98,
	0x9a, 0xda, 0x87, 0xf5, 0x8f, 0x6c, 0xca, 0x24, 0x2b, 0x97, 0x80, 0xfa, 0x3a, 0xa9, 0x5f, 0x9a,
	0x9d, 0x7a, 0x01, 0x70, 0xea, 0x0e, 0xa7, 0x16, 0x4e, 0xbc, 0x20, 0x4f, 0xa1, 0x19, 0xe5, 0x1c,
	0x05, 0x4a, 0x0a, 0xe9, 0x13, 0xa8, 0x71, 0x1c, 0xfb, 0x97, 0xd4, 0x38, 0x68, 0x45, 0xae, 0x8f,
	0xe7, 0xd5, 0xd5, 0x3d, 0x54, 0x1d, 0x44, 0x8e, 0xcb, 0xb3, 0x79, 0x03, 0xca, 0x11, 0x93, 0x81,
	0x1d, 0x73, 0xa2, 0xbb, 0xb9, 0xf1, 0x02, 0xae, 0x8a, 0x2c, 0xef, 0xb9, 0x50, 0xe7, 0xb0, 0x7d,
	0xc4, 0xa4, 0x66, 0xcf, 0xd0, 0xe5, 0xb7, 0x21, 0xec, 0xa1, 0x47, 0x8e, 0xed, 0x0a, 0x1f, 0xad,
	0xce, 0xd1, 0x50, 0xa0, 0xba, 0x05, 0x6e, 0xef, 0x39, 0x9b, 0xdf, 0xd0, 0x39, 0x73, 0x87, 0x38,
	0x72, 0xac, 0x21, 0x4b, 0xe4, 0x52, 0xae, 0x31, 0xae, 0xcd, 0x89, 0xf6, 0xa0, 0xa1, 0xb3, 0xa1,
	0x31, 0x35, 0x66, 0x23, 0xf6, 0xd9, 0x54, 0x6a, 0xfe, 0x8c, 0xc5, 0x45, 0xea, 0x61, 0x6e, 0xf8,
	0xf2, 0xed, 0xf3, 0x15, 0x5a, 0xa1, 0x7d, 0x7c, 0x70, 0x22, 0x52, 0x95, 0x34, 0xa9, 0x44, 0x91,
	0xaa, 0xe9, 0xae, 0x7a, 0x9b, 0x71, 0x58, 0x9e, 0xcc, 0x00, 0x36, 0x75, 0x36, 0xb6, 0x50, 0x32,
	0x67, 0x51, 0xc7, 0x1b, 0xe9, 0xa8, 0xef, 0x72, 0x8c, 0xca, 0x87, 0x7c, 0x05, 0x9d, 0xf3, 0xd9,
	0xed, 0x83, 0x1e, 0xe6, 0x9a, 0x95, 0x0f, 0xfb, 0x12, 0x5a, 0x9a, 0xcd, 0xb9, 0x25, 0x75, 0x66,
	0x98, 0xbf, 0x4e, 0x5c, 0xee, 0x85, 0xec, 0xc2, 0xca, 0xe2, 0xe8, 0x3b, 0xa8, 0xeb, 0xe1, 0x59,
	0x7d, 0x0f, 0xed, 0x8c, 0xc1, 0x2d, 0xe2, 0x21, 0x74, 0xb5, 0x89, 0x31, 0x1b, 0xb3, 0xdc, 0xb1,
	0x4b, 0x36, 0x7e, 0xa5, 0x54, 0xe3, 0xa7, 0x7b, 0xb3, 0x9a, 0xed, 0x4d, 0xad, 0x38, 0x68, 0x79,
	0xe6, 0xae, 0xb7, 0x14, 0x71, 0x72, 0x8c, 0xe3, 0xbb, 0xdd, 0xb6, 0xc9, 0x02, 0xd7, 0xd2, 0x05,
	0xfe, 0x53, 0x89, 0xe2, 0x96, 0xdf, 0x20, 0x19, 0x7a, 0xd5, 0x3c, 0x7a, 0x6d, 0xa8, 0x1f, 0xe3,
	0x38, 0x9c, 0xe7, 0xf9, 0x21, 0xc9, 0x67, 0x29, 0xcd, 0xc7, 0x80, 0x76, 0xb0, 0xa9, 0x2f, 0x2c,
	0x39, 0xb1, 0x5d, 0xf9, 0x01, 0x2f, 0xef, 0xf8, 0xf9, 0x39, 0x84, 0x4e, 0x4e, 0x88, 0xd2, 0xc9,
	0x3f, 0xff, 0x5b, 0x87, 0x8d, 0xd4, 0x17, 0x07, 0x6d, 0x03, 0xd1, 0xb8, 0x99, 0xf8, 0xae, 0x20,
	0xff, 0x65, 0xa5, 0x28, 0x48, 0x85, 0x76, 0xa0, 0xa5, 0x71, 0x33, 0xf9, 0x86, 0x93, 0x6a, 0x8e,
	0x18, 0x05, 0xa9, 0xd1, 0x47, 0xb0, 0xab, 0x71, 0xb3, 0xe8, 0xe9, 0x23, 0x4b, 0xd7, 0x2a, 0xa0,
	0x20, 0x75, 0xda, 0x82, 0xa6, 0xc6, 0xcd, 0xe8, 0x29, 0x27, 0xcb, 0x29, 0x11, 0x0a, 0xf2, 0x3f,
	0x7d, 0x08, 0xdd, 0xb9, 0x9b, 0xbc, 0xc9, 0x21, 0x2b, 0xd7, 0xe1, 0x28, 0xc8, 0x2a, 0xdd, 0x03,
	0x45, 0xe3, 0x66, 0xee, 0x13, 0x41, 0xa0, 0x18, 0x45, 0x41, 0x1a, 0x54, 0x81, 0x76, 0x1c, 0x0d,
	0x2f, 0x65, 0x2d, 0x1f, 0x41, 0x41, 0x9a, 0xb4, 0x0b, 0x5b, 0xde, 0xa7, 0x5e, 0x76, 0xa3, 0x91,
	0xf5, 0x22, 0x0c, 0x05, 0xd9, 0x08, 0x98, 0xe4, 0xee, 0x42, 0x42, 0x8a, 0x51, 0x14, 0xa4, 0x15,
	0xb0, 0xc9, 0xec, 0x27, 0xd2, 0xce, 0x47, 0x50, 0x90, 0x0e, 0x7d, 0x0c, 0x0f, 0x3c, 0xa4, 0x70,
	0x29, 0x91, 0xad, 0x1b, 0x54, 0x50, 0x90, 0xed, 0xb0, 0x60, 0x8b, 0x05, 0x41, 0x94, 0x94, 0x08,
	0x05, 0xd9, 0xa1, 0xbb, 0xb0, 0x1d, 0x95, 0x35, 0x31, 0x3f, 0xa4, 0x5b, 0x08, 0xa2, 0x20, 0xbb,
	0xc3, 0x65, 0xbf, 0xd9, 0x07, 0xff, 0x02, 0x00, 0x00, 0xff, 0xff, 0x2c, 0x8a, 0xe2, 0x34, 0xa5,
	0x0b, 0x00, 0x00,
}