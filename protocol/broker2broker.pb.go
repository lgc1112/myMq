// Code generated by protoc-gen-go.
// source: broker2broker.proto
// DO NOT EDIT!

/*
Package protocol is a generated protocol buffer package.

It is generated from these files:
	broker2broker.proto
	clientServer.proto
	message.proto
	metaData.proto

It has these top-level messages:
	Controller2Broker
	Broker2Controller
	ClientServerHeader
	CreatTopicReq
	CreatTopicRsp
	DeleteTopicReq
	DeleteTopicRsp
	PublishReq
	PublishRsp
	GetPublisherPartitionReq
	GetPublisherPartitionRsp
	GetConsumerPartitionReq
	GetConsumerPartitionRsp
	SubscribePartitionReq
	SubscribePartitionRsp
	SubscribeTopicReq
	SubscribeTopicRsp
	RegisterConsumerReq
	RegisterConsumerRsp
	UnRegisterConsumerReq
	UnRegisterConsumerRsp
	CommitReadyNumReq
	CommitReadyNumReqRsp
	ChangeConsumerPartitionReq
	ChangeConsumerPartitionRsp
	PushMsgReq
	PushMsgRsp
	PublishWithoutAskReq
	PublishWithoutAskResp
	Message
	InternalMessage
	Partition
	Partitions
	ListenAddr
	MetaData
	Topic
	Group
*/
package protocol

import proto "github.com/golang/protobuf/proto"
import fmt "fmt"
import math "math"

// Reference imports to suppress errors if they are not otherwise used.
var _ = proto.Marshal
var _ = fmt.Errorf
var _ = math.Inf

// This is a compile-time assertion to ensure that this generated file
// is compatible with the proto package it is being compiled against.
// A compilation error at this line likely means your copy of the
// proto package needs to be updated.
const _ = proto.ProtoPackageIsVersion2 // please upgrade the proto package

// broker与controller之间的数据
type Controller2BrokerKey int32

const (
	Controller2BrokerKey_CreadtPartition Controller2BrokerKey = 0
	Controller2BrokerKey_DeletePartition Controller2BrokerKey = 1
)

var Controller2BrokerKey_name = map[int32]string{
	0: "CreadtPartition",
	1: "DeletePartition",
}
var Controller2BrokerKey_value = map[string]int32{
	"CreadtPartition": 0,
	"DeletePartition": 1,
}

func (x Controller2BrokerKey) String() string {
	return proto.EnumName(Controller2BrokerKey_name, int32(x))
}
func (Controller2BrokerKey) EnumDescriptor() ([]byte, []int) { return fileDescriptor0, []int{0} }

type Broker2ControllerKey int32

const (
	Broker2ControllerKey_Heartbeat              Broker2ControllerKey = 0
	Broker2ControllerKey_RegisterBroker         Broker2ControllerKey = 1
	Broker2ControllerKey_unRegisterBroker       Broker2ControllerKey = 2
	Broker2ControllerKey_CreadtPartitionSuccess Broker2ControllerKey = 3
	Broker2ControllerKey_CreadtPartitionError   Broker2ControllerKey = 4
)

var Broker2ControllerKey_name = map[int32]string{
	0: "Heartbeat",
	1: "RegisterBroker",
	2: "unRegisterBroker",
	3: "CreadtPartitionSuccess",
	4: "CreadtPartitionError",
}
var Broker2ControllerKey_value = map[string]int32{
	"Heartbeat":              0,
	"RegisterBroker":         1,
	"unRegisterBroker":       2,
	"CreadtPartitionSuccess": 3,
	"CreadtPartitionError":   4,
}

func (x Broker2ControllerKey) String() string {
	return proto.EnumName(Broker2ControllerKey_name, int32(x))
}
func (Broker2ControllerKey) EnumDescriptor() ([]byte, []int) { return fileDescriptor0, []int{1} }

type Controller2Broker struct {
	Key        Controller2BrokerKey `protobuf:"varint,1,opt,name=key,enum=protocol.Controller2BrokerKey" json:"key,omitempty"`
	Partitions *Partition           `protobuf:"bytes,2,opt,name=Partitions" json:"Partitions,omitempty"`
}

func (m *Controller2Broker) Reset()                    { *m = Controller2Broker{} }
func (m *Controller2Broker) String() string            { return proto.CompactTextString(m) }
func (*Controller2Broker) ProtoMessage()               {}
func (*Controller2Broker) Descriptor() ([]byte, []int) { return fileDescriptor0, []int{0} }

func (m *Controller2Broker) GetKey() Controller2BrokerKey {
	if m != nil {
		return m.Key
	}
	return Controller2BrokerKey_CreadtPartition
}

func (m *Controller2Broker) GetPartitions() *Partition {
	if m != nil {
		return m.Partitions
	}
	return nil
}

type Broker2Controller struct {
	Key  Broker2ControllerKey `protobuf:"varint,1,opt,name=key,enum=protocol.Broker2ControllerKey" json:"key,omitempty"`
	Addr *ListenAddr          `protobuf:"bytes,2,opt,name=addr" json:"addr,omitempty"`
}

func (m *Broker2Controller) Reset()                    { *m = Broker2Controller{} }
func (m *Broker2Controller) String() string            { return proto.CompactTextString(m) }
func (*Broker2Controller) ProtoMessage()               {}
func (*Broker2Controller) Descriptor() ([]byte, []int) { return fileDescriptor0, []int{1} }

func (m *Broker2Controller) GetKey() Broker2ControllerKey {
	if m != nil {
		return m.Key
	}
	return Broker2ControllerKey_Heartbeat
}

func (m *Broker2Controller) GetAddr() *ListenAddr {
	if m != nil {
		return m.Addr
	}
	return nil
}

func init() {
	proto.RegisterType((*Controller2Broker)(nil), "protocol.Controller2Broker")
	proto.RegisterType((*Broker2Controller)(nil), "protocol.Broker2Controller")
	proto.RegisterEnum("protocol.Controller2BrokerKey", Controller2BrokerKey_name, Controller2BrokerKey_value)
	proto.RegisterEnum("protocol.Broker2ControllerKey", Broker2ControllerKey_name, Broker2ControllerKey_value)
}

func init() { proto.RegisterFile("broker2broker.proto", fileDescriptor0) }

var fileDescriptor0 = []byte{
	// 273 bytes of a gzipped FileDescriptorProto
	0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0xff, 0x74, 0x8f, 0x41, 0x4b, 0xfb, 0x40,
	0x10, 0xc5, 0xbb, 0x6d, 0xf9, 0xf3, 0x77, 0xa4, 0x75, 0x3b, 0x09, 0x12, 0x7a, 0x90, 0xd2, 0x53,
	0xe8, 0x21, 0x48, 0xfa, 0x05, 0xd4, 0x2a, 0x08, 0x7a, 0x90, 0xf8, 0x09, 0x36, 0xd9, 0xa1, 0x84,
	0xc6, 0xac, 0xcc, 0x6e, 0x0f, 0xf5, 0xee, 0xf7, 0x96, 0x24, 0x9a, 0x48, 0x5a, 0x4f, 0x03, 0xef,
	0xfd, 0xde, 0xbc, 0x19, 0xf0, 0x52, 0x36, 0x3b, 0xe2, 0xb8, 0x19, 0xd1, 0x3b, 0x1b, 0x67, 0xf0,
	0x7f, 0x3d, 0x32, 0x53, 0xcc, 0x27, 0x6f, 0x64, 0xad, 0xda, 0x52, 0x63, 0x2c, 0x3f, 0x60, 0xb6,
	0x31, 0xa5, 0x63, 0x53, 0x14, 0xc4, 0xf1, 0x5d, 0x9d, 0xc1, 0x6b, 0x18, 0xed, 0xe8, 0x10, 0x88,
	0x85, 0x08, 0xa7, 0xf1, 0x55, 0xf4, 0x93, 0x8d, 0x8e, 0xc8, 0x27, 0x3a, 0x24, 0x15, 0x8a, 0x6b,
	0x80, 0x17, 0xc5, 0x2e, 0x77, 0xb9, 0x29, 0x6d, 0x30, 0x5c, 0x88, 0xf0, 0x3c, 0xf6, 0xba, 0x60,
	0xeb, 0x25, 0xbf, 0xb0, 0xa5, 0x81, 0x59, 0xb3, 0x26, 0xee, 0x16, 0xff, 0xd9, 0x7d, 0x44, 0xb6,
	0xdd, 0x21, 0x8c, 0x95, 0xd6, 0xfc, 0xdd, 0xea, 0x77, 0x91, 0xe7, 0xdc, 0x3a, 0x2a, 0x6f, 0xb5,
	0xe6, 0xa4, 0x26, 0x56, 0x37, 0xe0, 0x9f, 0x7a, 0x01, 0x3d, 0xb8, 0xd8, 0x30, 0x29, 0xed, 0xda,
	0xe3, 0xe4, 0xa0, 0x12, 0xef, 0xa9, 0x20, 0x47, 0x9d, 0x28, 0x56, 0x9f, 0x02, 0xfc, 0x53, 0x97,
	0xe0, 0x04, 0xce, 0x1e, 0x49, 0xb1, 0x4b, 0x49, 0x39, 0x39, 0x40, 0x84, 0x69, 0x42, 0xdb, 0xaa,
	0x9f, 0x1b, 0x5c, 0x0a, 0xf4, 0x41, 0xee, 0xcb, 0x9e, 0x3a, 0xc4, 0x39, 0x5c, 0xf6, 0xba, 0x5f,
	0xf7, 0x59, 0x46, 0xd6, 0xca, 0x11, 0x06, 0xe0, 0xf7, 0xbc, 0x07, 0x66, 0xc3, 0x72, 0x9c, 0xfe,
	0xab, 0x9f, 0x5c, 0x7f, 0x05, 0x00, 0x00, 0xff, 0xff, 0xa9, 0x74, 0x73, 0x28, 0xed, 0x01, 0x00,
	0x00,
}
